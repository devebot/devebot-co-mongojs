'use strict';

var events = require('events');
var util = require('util');

var async = require('async');
var mongojs = require('mongojs');
var ObjectId = mongojs.ObjectId;

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debugx = Devebot.require('pinbug')('devebot:co:mongojs:bridge');

var chores = require('./utils/chores');

var noop = function() {};

var Service = function(params) {
  debugx.isEnabled && debugx(' + constructor start ...');

  params = params || {};

  var self = this;

  var LX = params.logger || chores.emptyLogger;
  var LT = params.tracer;

  var tracking_code = params.tracking_code || (new Date()).toISOString();

  self.getTrackingCode = function() {
    return tracking_code;
  };

  var mongo_conf = params.connection_options || {};
  var mongo_connection_string = chores.buildMongodbUrl(mongo_conf);

  self.mongo_cols = params.cols || {};
  self.mongojs = mongojs(mongo_connection_string, lodash.values(self.mongo_cols));
  let _client = self.mongojs;

  self.getServiceInfo = function() {
    var conf = lodash.pick(mongo_conf, ['host', 'port', 'name', 'username', 'password']);
    lodash.assign(conf, { password: '***' });
    return {
      connection_info: conf,
      url: chores.buildMongodbUrl(conf),
      collection_defs: self.mongo_cols
    };
  };

  self.getServiceHelp = function() {
    var info = self.getServiceInfo();
    return [{
      type: 'record',
      title: 'MongoDB bridge',
      label: {
        connection_info: 'Connection options',
        url: 'URL',
        collection_defs: 'Collections'
      },
      data: {
        connection_info: JSON.stringify(info.connection_info, null, 2),
        url: info.url,
        collection_defs: JSON.stringify(info.collection_defs, null, 2)
      }
    }];
  };

  this.close = function() {
    var self = this;
    return self.mongojs.close();
  }

  this.stats = function() {
    var self = this;
    return Promise.promisify(function(callback) {
      _client.stats(callback);
    })();
  };
  
  this.getCollectionNames = function() {
    var self = this;
    return Promise.promisify(function(callback) {
      _client.getCollectionNames(function(err, collectionNames) {
        callback(err, collectionNames);
      });
    })();
  };
  
  this.countDocuments = function(entity, criteria) {
    var self = this;
    if (!lodash.isObject(criteria)) criteria = {};
    return Promise.promisify(function(callback) {
      _client.collection(entity).count(criteria, function(err, result) {
        callback(err, result);
      });
    })();
  };
  
  this.findDocuments = function(entity, criteria, start, limit) {
    var self = this;
    return Promise.promisify(function(from, size, callback) {
      _client.collection(entity).find(criteria).skip(from).limit(size).toArray(function(err, docs) {
        callback(err, docs);
      });
    })(start, limit);
  };
  
  this.getDocuments = function(entity, start, limit) {
    var self = this;
    return Promise.promisify(function(from, size, callback) {
      _client.collection(entity).find({
      }).skip(from).limit(size).toArray(function(err, docs) {
        callback(err, docs);
      });
    })(start, limit);
  };
  
  this.findOneDocument = function(entity, criteria) {
    var self = this;
    return Promise.promisify(function(callback) {
      _client.collection(entity).findOne(criteria, function(err, obj) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - findOneDocument("%s", "%s") has error: %s', self.getTrackingCode(),
              entity, JSON.stringify(criteria), JSON.stringify(err));
        } else {
          LX.has('info') && LX.log('info', '<%s> - findOneDocument("%s", "%s") result: %s', self.getTrackingCode(),
              entity, JSON.stringify(criteria), JSON.stringify(obj));
        }
        callback(err, obj);
      });
    })();
  };
  
  this.getDocumentById = function(entity, id) {
    var self = this;
    return Promise.promisify(function(callback) {
      if (lodash.isEmpty(id)) {
        callback({name: 'documentId_is_empty', entity: entity});
        return;
      }
      if (!(id instanceof ObjectId)) {
        id = ObjectId(id);
      }
      _client.collection(entity).findOne({
        _id: id
      }, function(err, obj) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - getDocumentById("%s", "%s") has error: %s', self.getTrackingCode(),
              entity, id, JSON.stringify(err));
        } else {
          LX.has('info') && LX.log('info', '<%s> - getDocumentById("%s", "%s") result: %s', self.getTrackingCode(),
              entity, id, JSON.stringify(obj));
        }
        callback(err, obj);
      });
    })();
  };
  
  this.getDocumentsByIds = function(entityName, documentIds) {
    var self = this;
    LX.has('info') && LX.log('info', '<%s> + getDocumentsByIds("%s", "%s")', self.getTrackingCode(),
        entityName, JSON.stringify(documentIds));
  
    return Promise.promisify(function(callback) {
  
      if (!lodash.isArray(documentIds)) {
        callback('documentIds_is_not_an_array');
        return;
      }
  
      documentIds = lodash.map(documentIds, function(documentId) {
        if (documentId instanceof ObjectId) {
          return documentId;
        } else {
          return ObjectId(documentId);
        }
      });
  
      _client.collection(entityName).find({
        _id: {
          $in: documentIds
        }
      }, function(err, objs) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - getDocumentsByIds("%s", "%s") has error: %s', self.getTrackingCode(),
              entityName, JSON.stringify(documentIds), JSON.stringify(err));
        } else {
          LX.has('info') && LX.log('info', '<%s> - getDocumentsByIds("%s", "%s") result: %s', self.getTrackingCode(),
              entityName, JSON.stringify(documentIds), JSON.stringify(objs));
        }
        callback(err, objs);
      });
    })();
  };
  
  this.getOneToManyTargetsBySourceId = function(entityName, sourceIdName, sourceId) {
    var self = this;
    return Promise.promisify(function(callback) {
      if (!(sourceId instanceof ObjectId)) {
        sourceId = ObjectId(sourceId);
      }
  
      var criteria = {};
      criteria[sourceIdName] = sourceId;
  
      _client.collection(entityName).find(criteria).toArray(function(err, docs) {
        callback(err, docs);
      });
    })();
  };
  
  this.getHierarchicalDocumentsToTop = function(entity, documentId) {
    var self = this;
    var documents = [];
    return Promise.promisify(function(callback) {
      async.whilst(function() {
        return (!lodash.isEmpty(documentId));
      }, function(done_doWhilst) {
        self.getDocumentById(entity, documentId).then(function(document) {
          if (lodash.isObject(document)) {
            documents.push(document);
            documentId = document.parentId;
          } else {
            documentId = null;
          }
          done_doWhilst();
        });
      }, function(error_doWhilst) {
        callback(error_doWhilst, documents);
      });
    })();
  };
  
  this.getChainToTopOfHierarchicalDocumentsByIds = function(entityName, documentIds) {
    var self = this;
    var documents = [];
    var scanDocuments = function(callback) {
      async.eachSeries(documentIds, function(documentId, done_each) {
        self.getHierarchicalDocumentsToTop(entityName, documentId).then(function(chain) {
          if (lodash.isArray(chain) && chain.length > 0) {
            documents.push({
              documentId: documentId,
              documentObject: chain[0],
              documentChain: chain
            });
          }
          done_each();
        });
      }, function(error_each) {
        callback(error_each, documents);
      });
    };
    return Promise.promisify(scanDocuments)();
  };
  
  this.insertDocument = function(entity, documents) {
    var self = this;
    return Promise.promisify(function (done) {
      _client.collection(entity).insert(documents, function(err, result) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - insert documents %s of %s error: %s', self.getTrackingCode(),
              JSON.stringify(documents), entity, err);
        } else {
          LX.has('info') && LX.log('info', '<%s> - insert documents %s of %s successful: ', self.getTrackingCode(),
              JSON.stringify(documents), entity, JSON.stringify(result));
        }
        done(err, result);
      });
    })();
  };
  
  this.updateDocument = function(entity, criteria, data, options) {
    var self = this;
    options = options || {multi: true, upsert: false};
    var promisee = function (done) {
      _client.collection(entity).update(criteria, {$set: data}, options, function(err, info) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - update %s document: %s with options %s and criteria %s has error: %s', self.getTrackingCode(),
              entity, JSON.stringify(data), JSON.stringify(options), JSON.stringify(criteria), err);
        } else {
          LX.has('info') && LX.log('info', '<%s> - update %s document: %s with options %s and criteria %s successul: %s', self.getTrackingCode(),
              entity, JSON.stringify(data), JSON.stringify(options), JSON.stringify(criteria), JSON.stringify(info));
        }
        done(err, info);
      });
    };
    return Promise.promisify(promisee)();
  };
  
  this.deleteDocument = function(entityName, criteria) {
    var self = this;
    var promisee = function (done) {
      _client.collection(entityName).remove(criteria, function(err, result) {
        if (err) {
          LX.has('info') && LX.log('info', '<%s> - delete %s document with criteria %s has error: %s', self.getTrackingCode(),
              entityName, JSON.stringify(criteria), err);
        } else {
          LX.has('info') && LX.log('info', '<%s> - delete %s document with criteria %s result: %s', self.getTrackingCode(),
              entityName, JSON.stringify(criteria), JSON.stringify(result));
        }
        done(err, result);
      });
    };
    return Promise.promisify(promisee)();
  };
  
  this.getDocumentSummary = function() {
    var self = this;
    return Promise.resolve().then(function() {
      return self.getCollectionNames();
    }).then(function(collectionNames) {
      var coldefs = self.getServiceInfo().collection_defs;
      var collection_names = lodash.intersection(lodash.values(coldefs), collectionNames);
      return Promise.mapSeries(collection_names, function(collection_name) {
        return self.countDocuments(collection_name);
      }).then(function(counts) {
        counts = counts || [];
        var countLabels = {};
        var countResult = {};
        for(var i=0; i<counts.length; i++) {
          countLabels[collection_names[i]] = collection_names[i];
          countResult[collection_names[i]] = counts[i];
        }
        return Promise.resolve({
          label: countLabels,
          count: countResult
        });
      });
    });
  };

  debugx.isEnabled && debugx(' - constructor has finished');
};

module.exports = Service;

Service.metadata = require('./metadata');