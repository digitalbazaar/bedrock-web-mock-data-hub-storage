/*!
 * Copyright (c) 2018-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

import uuid from 'uuid-random';

export class MockStorage {
  constructor({server}) {
    this.dataHubs = new Map();
    this.primaryHubs = new Map();
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
      if(config.primary) {
        const primaryHub = this.primaryHubs.get(config.controller);
        if(primaryHub) {
          return [409];
        }
        this.primaryHubs.set(config.controller, dataHub);
      }
      const location = `http://localhost:9876/${root}/${config.id}`;
      return [201, {location, json: true}, config];
    });

    // get data hubs by query
    server.get(routes.dataHubs, request => {
      const {controller, primary} = request.queryParams;
      if(primary !== 'true') {
        // query for all data hubs controlled by controller not implemented yet
        // TODO: implement
        return [500, {json: true}, new Error('Not implemented.')];
      }
      const primaryHub = this.primaryHubs.get(controller);
      if(!primaryHub) {
        return [200, {json: true}, []];
      }
      return [200, {json: true}, [primaryHub.config]];
    });

    // get a data hub
    server.get(routes.dataHub, request => {
      const {dataHubId} = request.params;
      const dataHub = this.dataHubs.get(dataHubId);
      if(!dataHub) {
        return [404];
      }
      return [200, {json: true}, dataHub.config];
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
      try {
        this.store({dataHub, doc, create: true});
      } catch(e) {
        return [409];
      }
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

  store({dataHub, doc, create = false}) {
    if(create) {
      // check uniqueness constraint
      for(const entry of doc.indexed) {
        const index = dataHub.indexes.get(entry.hmac.id);
        if(!index) {
          continue;
        }
        for(const attribute of entry.attributes) {
          if(!attribute.unique) {
            continue;
          }
          const key = attribute.name + '=' + attribute.value;
          if(index.equals.has(key)) {
            throw new Error('Duplicate error.');
          }
        }
      }
    }

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
          doc,
          unique: attribute.unique
        });
        this.addToIndex({
          index: index.has,
          key: attribute.name,
          doc
        });
      }
    }
  }

  addToIndex({index, key, doc, unique = false}) {
    let docSet = index.get(key);
    if(!docSet) {
      docSet = new Set();
      index.set(key, docSet);
    }
    if(unique) {
      docSet.clear();
    }
    docSet.add(doc);
  }

  unindex({dataHub, doc}) {
    for(const entry of doc.indexed) {
      const index = dataHub.indexes.get(entry.hmac.id);
      for(const attribute of entry.attributes) {
        this.removeFromIndex({
          index: index.equals,
          key: attribute.name + '=' + attribute.value
        });
        this.removeFromIndex({
          index: index.has,
          key: attribute.name
        });
      }
    }
  }

  removeFromIndex({index, key}) {
    let docSet = index.get(key);
    if(docSet) {
      index.delete(key);
    }
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
      return [204];
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
      const doc = dataHub.documents.get(docId);
      this.unindex({dataHub, doc});
      dataHub.documents.delete(docId);
      return [204];
    });
  }
}
