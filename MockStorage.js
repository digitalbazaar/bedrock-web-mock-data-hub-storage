/*!
 * Copyright (c) 2018-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

import uuid from 'uuid-random';

export class MockStorage {
  constructor({server}) {
    this.dataHubs = new Map();
    this.documents = new Map();
    this.indexes = new Map();

    const root = '/data-hubs';
    const routes = {
      dataHubs: root,
      dataHub: new RegExp(`${root}/:dataHubId`),
      documents: new RegExp(`${root}/:dataHubId/documents`),
      document: new RegExp(`${root}/:dataHubId/documents/:docId`),
      dataHub: new RegExp(`${root}/:dataHubId/query`)
    };

    server.map(() => {
      // create a new data hub
      server.post(routes.dataHubs, request => {
        const config = JSON.parse(request.requestBody);
        // TODO: validate `config`
        config.id = uuid();
        this.dataHubs.set(config.id, {
          config,
          documents: new Map(),
          indexes: {
            equals: new Map(),
            has: new Map()
          }
        });
        const location = `http://localhost:9876/${root}/${config.id}`;
        return [200, {location, json: true}, config];
      });

      // insert a document into a data hub
      server.post(routes.documents, request => {
        const {dataHubId} = request.params;
        const dataHub = this.dataHubs.get(dataHubId);
        if(!dataHub) {
          // data hub does not exist
          return [404];
        }

        const doc = JSON.parse(request.requestBody);
        if(this.dataHub.has(doc.id)) {
          return [409];
        }
        this.store(dataHub, doc);
        const location =
          `http://localhost:9876/${root}/${dataHubId}/documents/${doc.id}`;
        return [201, {location}];
      });

      server.post(routes.document, request => {
        const {dataHubId, docId} = request.params;
        const dataHub = this.dataHubs.get(dataHubId);
        if(!dataHub) {
          // data hub does not exist
          return [404];
        }

        const doc = JSON.parse(request.requestBody);
        if(docId !== doc.id) {
          return [400];
        }
        this.store(dataHub, doc);
        return [200];
      });

      server.get(routes.document, request => {
        const {dataHubId, docId} = request.params;
        const dataHub = this.dataHubs.get(dataHubId);
        if(!dataHub) {
          // data hub does not exist
          return [404];
        }

        const doc = dataHub.documents.get(docId);
        if(!doc) {
          return [404];
        }
        return [200, {json: true}, doc];
      });

      server.delete(routes.document, request => {
        const {dataHubId, docId} = request.params;
        const dataHub = this.dataHubs.get(dataHubId);
        if(!dataHub) {
          // data hub does not exist
          return [404];
        }

        if(!dataHub.documents.has(docId)) {
          return [404];
        }
        dataHub.documents.delete(docId);
        return [204];
      });

      server.post(routes.query, request => {
        const {dataHubId} = request.params;
        const dataHub = this.dataHubs.get(dataHubId);
        if(!dataHub) {
          // data hub does not exist
          return [404];
        }

        const query = JSON.parse(request.requestBody);
        const index = dataHub.indexes[query.index];
        if(!index) {
          // index does not exist
          return [404];
        }

        // build results
        const results = [];
        if(query.equals) {
          for(const equals of query.equals) {
            let matches = null;
            for(const key in equals) {
              const value = equals[key];
              const docs = this.find(
                {index: dataHub.indexes.equals, key: key + '=' + value});
              if(!matches) {
                // first result
                matches = docs;
              } else {
                // remove any docs from `matches` that are not in `docs`
                matches = matches.filter(x => docs.includes(x));
                if(matches.length === 0) {
                  break;
                }
              }
            }
            (matches || []).forEach(x => {
              if(!results.includes(x)) {
                results.push(x);
              }
            });
          }
        }

        if(query.has) {
          let matches = null;
          for(const key of query.has) {
            const docs = dataHub.find({index: this.indexes.has, key});
            if(!matches) {
              // first result
              matches = docs;
            } else {
              // remove any docs from `matches` that are not in `docs`
              matches = matches.filter(x => docs.includes(x));
              if(matches.length === 0) {
                break;
              }
            }
          }
          results.push(...(matches || []));
        }

        return [200, {json: true}, results];
      });
    });
  }

  store(dataHub, doc) {
    dataHub.documents.set(doc.id, doc);
    for(const entry of doc.indexed) {
      let index = dataHub.indexes[entry.hmac.id];
      if(!index) {
        index = {
          equals: new Map(),
          has: new Map()
        };
        dataHub.indexes.set(entry.hmac.id, index);
      }
      for(const attribute of entry.attributes) {
        this.addToIndex({
          index: index.equals,
          key: attribute.name + '=' + attribute.value,
          doc
        });
        this.addToIndex({
          index: index.has,
          key: attribute.name,
          doc
        });
      }
    }
  }

  addToIndex({index, key, doc}) {
    let docSet = index.get(key);
    if(!docSet) {
      docSet = new Set();
      index.set(key, docSet);
    }
    docSet.add(doc);
  }

  find({index, key}) {
    const docSet = index.get(key);
    if(!docSet) {
      return [];
    }
    return [...docSet];
  }
}
