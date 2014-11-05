var underscore = require('underscore');
var mongodb = require('mongodb');
var async = require('async');
var events = require('events');
var helper = require('./helper');

var config = require('./config');
var stores = {};

module.exports.initialize = function(netnames, callback, complete) {
  var tasks = netnames.map(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    return function(c) {
      store.connect(function(err, conn) {
	if(err) return c(err);
	if(typeof callback == 'function') callback(err, netname);
	c(err, netname);
      });
    };
  });
  async.parallel(tasks, complete);
};

function Store(netname) {
  var self = this;
  this.netname = netname;
  this.events = new events.EventEmitter();
  this.max_height = -1;
  this.tip_timestamp = 0;
}

Store.prototype.queryMaxHeight = function(callback) {
  var self = this;
  this.getTipBlock(function(err, tip) {
    if(err) return callback(err);
    if(tip) {
      self.max_height = tip.height;
      self.tip_timestamp = tip.timestamp;
      return callback(undefined, tip.height);
    } else {
      return callback(undefined, self.max_height);
    }
  });
}

Store.prototype.on = function(event, listener) {
  this.events.on(event, listener);
};

Store.prototype.emit = function() {
  this.events.emit.apply(this.events, arguments);
};

Store.prototype.getVar = function(key, callback) {
  var col = this.dbconn.collection('var');
  col.findOne({key: key}, callback);
};

Store.prototype.saveVar = function(v, callback) {
  delete v._id;
  var col = this.dbconn.collection('var');
  col.findAndModify({key: v.key}, [], {$set: v}, {upsert: true, "new": true},
		    callback);

};

Store.prototype.connect = function(callback) {
  var self = this;
  var url = config.networks[this.netname].db.url;
  mongodb.MongoClient.connect(url, function(err, aConn) {
    if(err) {
      console.error(err);
      if(typeof callback == 'function') return callback(err);
    }
    self.dbconn = aConn;
    
    function nf(err) {if(err){console.error(err);}}

    var blockIndexCol = self.dbconn.collection('blockindex');
    blockIndexCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    blockIndexCol.ensureIndex({'prev_hash': 1}, {}, nf);

    var blockCol = self.dbconn.collection('block');
    blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    blockCol.ensureIndex({'height': -1}, nf);

    var txCol = self.dbconn.collection('tx');
    txCol.ensureIndex({'hash': 1}, {unique: true}, nf);
    //txCol.ensureIndex({'bhash': 1}, {}, nf);
    txCol.ensureIndex({'bhs': 1}, {}, nf);
    txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1},
		      {}, nf);
    txCol.ensureIndex({'vin.k': 1}, {}, nf);
    //txCol.ensureIndex({'vout.spt': 1}, {}, nf);
    txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var mpCol = self.dbconn.collection('mempool');
    mpCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    mpCol.ensureIndex({'pending': 1}, {}, nf);
    mpCol.ensureIndex({'vin.k': 1}, {}, nf);
    mpCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var varCol = self.dbconn.collection('var');
    varCol.ensureIndex({'key': 1}, {unique: 1}, nf);

    self.queryMaxHeight(function(err, height) {
      if(err) throw callback(err);
      callback(undefined, self.dbconn);      
    });
  });
};

Store.prototype.getBlock = function(hash, callback) {
  var self = this;
  if(!hash) return callback();

  var col = this.dbconn.collection('block');
  col.findOne({hash: hash}, function(err, block) {
    if(err) {
      return callback(err);
    }
    if(block) {
      block.hash = helper.toBuffer(block.hash);
      block.prev_hash = helper.toBuffer(block.prev_hash);
      if(block.next_hash) {
	block.next_hash = helper.toBuffer(block.next_hash);
      }
    }
    callback(undefined, block);
  });
};

Store.prototype.getTipBlock = function(callback) {
  var self = this;
  var col = this.dbconn.collection('var');
  col.findOne({key: 'tip'}, function(err, obj) {
    if(err) return callback(err);
    if(obj) {
      self.getBlock(obj.blockHash, callback);
    } else {
      callback();
    }
  });
};

Store.prototype.setTipBlock = function(block, callback) {
  var col = this.dbconn.collection('var');
  col.update({'key': 'tip'}, {$set: {blockHash: block.hash}},
	     {upsert: true}, callback);
};

Store.prototype.saveBlock = function(block, callback) {
  var self = this;
  var col = this.dbconn.collection('block');
  col.save(block, callback);
};

Store.prototype.getTx = function(hash, withBlocks, callback) {
  return this.getTxes([hash], withBlocks, function(err, arr){
    if(err) return callback(err);
    if(arr.length == 0) {
      return callback(err, null);
    } else {
      return callback(err, arr[0]);
    }
  });
};

Store.prototype.getTxes = function(txHashList, callback) {
  var self = this;
  var txCol = this.dbconn.collection('tx');


  if(txHashList.length <= 0) {
    return callback(undefined, []);
  } else if(txHashList.length == 1) {
    var query = {hash: txHashList[0]};
  } else {
    var query = {hash: {$in: txHashList}};
  }
  txCol.find(query).toArray(function(err, txes) {
    if(err) return callback(err);
    txes.forEach(function(tx) {
      tx.hash = helper.toBuffer(tx.hash);
    });
    return callback(err, txes);
  });
};

Store.prototype.getBlocks = function(txes, callback) {
  var blockHashes = [];
  var blockCol = this.dbconn.collection('block');
  txes.forEach(function(tx) {
    tx.bhs.forEach(function(bh) {
      blockHashes.push(bh);
    });
  });
  blockHashes = underscore.uniq(blockHashes);

  if(blockHashes.length > 0) {
    blockCol.find({hash: {$in: blockHashes}}).toArray(function(err, arr) {
      var blocks = {};
      arr.forEach(function(block) {
	blocks[block.hash.toString('hex')] = block;
      });
      txes.forEach(function(tx) {
	tx.blocks = [];
	tx.bhs.forEach(function(bh) {
	  tx.blocks.push(blocks[bh.toString('hex')]);
	});
      });
      return callback(err, txes);
    });
  } else {
    return callback(undefined, txes);
  }
};

module.exports.stores = stores;
