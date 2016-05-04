'use strict';

var events = require('events');
var util = require('util');

var mongojs = require('mongojs');
var ObjectId = mongojs.ObjectId;

var Devebot = require('devebot');
var async = Devebot.require('async');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:mongodb:mongodbBridge');

var chores = require('../utils/chores.js');

var noop = function() {};

var Service = function(params) {
  debuglog(' + constructor start ...');
  Service.super_.apply(this);
  
  params = params || {};
  
  var self = this;
  
  self.logger = self.logger || params.logger || { trace: noop, info: noop, debug: noop, warn: noop, error: noop };
  
  var tracking_code = params.tracking_code || (new Date()).toISOString();
  
  self.getTrackingCode = function() {
    return tracking_code;
  };
  
  var mongo_conf = params.connection_options || {};
  var mongo_connection_string = chores.buildMongodbUrl(mongo_conf);

  self.mongo_cols = params.cols || {};
  self.mongodb = mongojs(mongo_connection_string, lodash.values(self.mongo_cols));

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

  debuglog(' - constructor has finished');
};

Service.argumentSchema = {
  "id": "/mongodbBridge",
  "type": "object",
  "properties": {
    "tracking_code": {
      "type": "string"
    },
    "connection_options": {
      "type": "object"
    }
  }
};

util.inherits(Service, events.EventEmitter);

Service.prototype.stats = function() {
  var self = this;
  return Promise.promisify(function(callback) {
    self.mongodb.stats(callback);
  })();
};

Service.prototype.getCollectionNames = function() {
  var self = this;
  return Promise.promisify(function(callback) {
    self.mongodb.getCollectionNames(function(err, collectionNames) {
      callback(err, collectionNames);
    });
  })();
};

Service.prototype.countDocuments = function(entity, criteria) {
  var self = this;
  if (!lodash.isObject(criteria)) criteria = {};
  return Promise.promisify(function(callback) {
    self.mongodb[entity].count(criteria, function(err, result) {
      callback(err, result);
    });
  })();
};

Service.prototype.findDocuments = function(entity, criteria, start, limit) {
  var self = this;
  return Promise.promisify(function(from, size, callback) {
    self.mongodb[entity].find(criteria).skip(from).limit(size).toArray(function(err, docs) {
      callback(err, docs);
    });
  })(start, limit);
};

Service.prototype.getDocuments = function(entity, start, limit) {
  var self = this;
  return Promise.promisify(function(from, size, callback) {
    self.mongodb[entity].find({
    }).skip(from).limit(size).toArray(function(err, docs) {
      callback(err, docs);
    });
  })(start, limit);
};

Service.prototype.findOneDocument = function(entity, criteria) {
  var self = this;
  return Promise.promisify(function(callback) {
    self.mongodb[entity].findOne(criteria, function(err, obj) {
      if (err) {
        self.logger.info('<%s> - findOneDocument("%s", "%s") has error: %s', self.getTrackingCode(),
            entity, JSON.stringify(criteria), JSON.stringify(err));
      } else {
        self.logger.info('<%s> - findOneDocument("%s", "%s") result: %s', self.getTrackingCode(),
            entity, JSON.stringify(criteria), JSON.stringify(obj));
      }
      callback(err, obj);
    });
  })();
};

Service.prototype.getDocumentById = function(entity, id) {
  var self = this;
  return Promise.promisify(function(callback) {
    if (lodash.isEmpty(id)) {
      callback({name: 'documentId_is_empty', entity: entity});
      return;
    }
    if (!(id instanceof ObjectId)) {
      id = ObjectId(id);
    }
    self.mongodb[entity].findOne({
      _id: id
    }, function(err, obj) {
      if (err) {
        self.logger.info('<%s> - getDocumentById("%s", "%s") has error: %s', self.getTrackingCode(), 
            entity, id, JSON.stringify(err));
      } else {
        self.logger.info('<%s> - getDocumentById("%s", "%s") result: %s', self.getTrackingCode(),
            entity, id, JSON.stringify(obj));
      }
      callback(err, obj);
    });
  })();
};

Service.prototype.getDocumentsByIds = function(entityName, documentIds) {
  var self = this;
  self.logger.info('<%s> + getDocumentsByIds("%s", "%s")', self.getTrackingCode(), 
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

    self.mongodb[entityName].find({
      _id: {
        $in: documentIds
      }
    }, function(err, objs) {
      if (err) {
        self.logger.info('<%s> - getDocumentsByIds("%s", "%s") has error: %s', self.getTrackingCode(), 
            entityName, JSON.stringify(documentIds), JSON.stringify(err));
      } else {
        self.logger.info('<%s> - getDocumentsByIds("%s", "%s") result: %s', self.getTrackingCode(), 
            entityName, JSON.stringify(documentIds), JSON.stringify(objs));
      }
      callback(err, objs);
    });
  })();
};

Service.prototype.getOneToManyTargetsBySourceId = function(entityName, sourceIdName, sourceId) {
  var self = this;
  return Promise.promisify(function(callback) {
    if (!(sourceId instanceof ObjectId)) {
      sourceId = ObjectId(sourceId);
    }
    
    var criteria = {};
    criteria[sourceIdName] = sourceId;
    
    self.mongodb[entityName].find(criteria).toArray(function(err, docs) {
      callback(err, docs);
    });
  })();
};

Service.prototype.getHierarchicalDocumentsToTop = function(entity, documentId) {
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

Service.prototype.getChainToTopOfHierarchicalDocumentsByIds = function(entityName, documentIds) {
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

Service.prototype.insertDocument = function(entity, documents) {
  var self = this;
  return Promise.promisify(function (done) {
    self.mongodb[entity].insert(documents, function(err, result) {
      if (err) {
        self.logger.info('<%s> - insert documents %s of %s error: %s', self.getTrackingCode(),
            JSON.stringify(documents), entity, err);
      } else {
        self.logger.info('<%s> - insert documents %s of %s successful: ', self.getTrackingCode(),
            JSON.stringify(documents), entity, JSON.stringify(result));
      }
      done(err, result);
    });
  })();
};

Service.prototype.updateDocument = function(entity, criteria, data, options) {
  var self = this;
  options = options || {multi: true, upsert: false};
  var promisee = function (done) {
    self.mongodb[entity].update(criteria, {$set: data}, options, function(err, info) {
      if (err) {
        self.logger.info('<%s> - update %s document: %s with options %s and criteria %s has error: %s', self.getTrackingCode(),
            entity, JSON.stringify(data), JSON.stringify(options), JSON.stringify(criteria), err);
      } else {
        self.logger.info('<%s> - update %s document: %s with options %s and criteria %s successul: %s', self.getTrackingCode(),
            entity, JSON.stringify(data), JSON.stringify(options), JSON.stringify(criteria), JSON.stringify(info));
      }
      done(err, info);
    });
  };
  return Promise.promisify(promisee)();
};

Service.prototype.deleteDocument = function(entityName, criteria) {
  var self = this;
  var promisee = function (done) {
    self.mongodb[entityName].remove(criteria, function(err, result) {
      if (err) {
        self.logger.info('<%s> - delete %s document with criteria %s has error: %s', self.getTrackingCode(),
            entityName, JSON.stringify(criteria), err);
      } else {
        self.logger.info('<%s> - delete %s document with criteria %s result: %s', self.getTrackingCode(),
            entityName, JSON.stringify(criteria), JSON.stringify(result));
      }
      done(err, result);
    });
  }; 
  return Promise.promisify(promisee)();
};

Service.prototype.getDocumentSummary = function() {
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

module.exports = Service;
