/*!
 * Copyright (c) 2018-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

import uuid from 'uuid-random';

export class MockStorage {
  constructor({mockAdapter}) {
    this.dataHubs = new Map();
    this.documents = new Map();
    this.indexes = new Map();
    this.mockAdapter = mockAdapter;

    const root = '/data-hubs';
    const routes = {
      dataHubs: root,
      dataHub: new RegExp(`${root}/([-A-Za-z0-9]+)`),
      documents: new RegExp(`${root}/([-A-Za-z0-9]+)/documents`),
      document: new RegExp(`${root}/([-A-Za-z0-9]+)/documents/([-A-Za-z0-9]+)`),
      dataHub: new RegExp(`${root}/([-A-Za-z0-9]+)/query`)
    };

    // create a new data hub
    mockAdapter.onPost(routes.dataHubs).reply(request => {
      const config = JSON.parse(request.data);
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
      return [200, JSON.stringify(config), {location}];
    });

    // insert a document into a data hub
    mockAdapter.onPost(routes.documents).reply(request => {
      const [, dataHubId] = request.url.match(routes.documents);
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        // data hub does not exist
        return [404];
      }

      const doc = JSON.parse(request.data);
      if(this.dataHub.has(doc.id)) {
        return [409];
      }
      this.store(dataHub, doc);
      const location =
        `http://localhost:9876/${root}/${dataHubId}/documents/${doc.id}`;
      return [201, undefined, {location}];
    });

    mockAdapter.onPost(routes.document).reply(request => {
      const [, dataHubId, docId] = request.url.match(routes.document);
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        // data hub does not exist
        return [404];
      }

      const doc = JSON.parse(request.data);
      if(docId !== doc.id) {
        return [400];
      }
      this.store(dataHub, doc);
      return [200];
    });

    mockAdapter.onGet(routes.document).reply(request => {
      const [, dataHubId, docId] = request.url.match(routes.document);
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        // data hub does not exist
        return [404];
      }

      const doc = dataHub.documents.get(docId);
      if(!doc) {
        return [404];
      }
      return [200, JSON.stringify(doc)];
    });

    mockAdapter.onDelete(routes.document).reply(request => {
      const [, dataHubId, docId] = request.url.match(routes.document);
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

    mockAdapter.onPost(routes.query).reply(request => {
      const [, dataHubId] = request.url.match(routes.documents);
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        // data hub does not exist
        return [404];
      }

      const query = JSON.parse(request.data);
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

      return [200, results];
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
