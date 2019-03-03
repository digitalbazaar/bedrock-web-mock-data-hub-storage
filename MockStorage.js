/*!
 * Copyright (c) 2018-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

import uuid from 'uuid-random';

export class MockStorage {
  constructor({server}) {
    this.dataHubs = new Map();
    this.documents = new Map();

    const root = '/data-hubs';
    const routes = this.routes = {
      dataHubs: root,
      dataHub: `${root}/:dataHubId`,
      documents: `${root}/:dataHubId/documents`,
      query: `${root}/:dataHubId/query`
    };

    // create a new data hub
    server.post(routes.dataHubs, request => {
      const config = JSON.parse(request.requestBody);
      // TODO: validate `config`
      config.id = uuid();
      const dataHub = {
        config,
        documents: new Map(),
        indexes: new Map()
      };
      this.dataHubs.set(config.id, dataHub);
      this.mapDocumentHandlers({server, dataHub});
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
      if(dataHub.documents.has(doc.id)) {
        return [409];
      }
      this.store({dataHub, doc});
      const location =
        `http://localhost:9876/${root}/${dataHubId}/documents/${doc.id}`;
      return [201, {location}];
    });

    // query a data hub
    server.post(routes.query, request => {
      const {dataHubId} = request.params;
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        // data hub does not exist
        return [404];
      }

      const query = JSON.parse(request.requestBody);
      const index = dataHub.indexes.get(query.index);
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
              {index: index.equals, key: key + '=' + value});
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
          const docs = this.find({index: index.has, key});
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
  }

  store({dataHub, doc}) {
    dataHub.documents.set(doc.id, doc);
    for(const entry of doc.indexed) {
      let index = dataHub.indexes.get(entry.hmac.id);
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

  mapDocumentHandlers({server, dataHub}) {
    const root = `${this.routes.dataHubs}/${dataHub.config.id}/documents`;
    const route = `${root}/:docId`;

    // update a document
    server.post(route, request => {
      const {docId} = request.params;
      const doc = JSON.parse(request.requestBody);
      if(docId !== doc.id) {
        return [400];
      }
      this.store({dataHub, doc});
      return [200];
    });

    // get a document
    server.get(route, request => {
      const {docId} = request.params;
      const doc = dataHub.documents.get(docId);
      if(!doc) {
        return [404];
      }
      return [200, {json: true}, doc];
    });

    // delete a document
    server.delete(route, request => {
      const {docId} = request.params;
      if(!dataHub.documents.has(docId)) {
        return [404];
      }
      dataHub.documents.delete(docId);
      return [204];
    });
  }
}
