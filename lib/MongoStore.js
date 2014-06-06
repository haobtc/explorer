var mongodb = require('mongodb');
var helper = require('./helper');
var config = require('./config');
var stores = {};

module.exports.initialize = function(netnames, callback) {
  netnames.forEach(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    store.connect(function(err, conn) {
      if(typeof callback=='function') callback(err, netname);
    });
  });
};

function Store(netname) {
  this.netname = netname;
}

Store.prototype.toBuffer = function(hash) {
  if(hash instanceof mongodb.Binary) {
    return hash.buffer;
  } else if(hash instanceof Buffer) {
    return hash;
  } else {
    return new Buffer(hash, 'hex');
  }
};

Store.prototype.connect = function(callback) {
  var self = this;
  var url = config[this.netname].db.url;
  mongodb.MongoClient.connect(url, function(err, aConn) {
    if(err) {
      console.error(err);
      if(typeof callback == 'function') callback(err);
      return;
    }
    self.dbconn = aConn;
    
    function nf(err) {if(err){console.error(err);}}

    var blockCol = self.dbconn.collection('block');
    blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    blockCol.ensureIndex({'height': -1}, nf);

    var txCol = self.dbconn.collection('tx');
    txCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    txCol.ensureIndex({'bhash': 1}, {}, nf);
    txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1},
		      {unique: 1, sparse: 1}, nf);
    txCol.ensureIndex({'vin.k': 1}, {unique: 1, sparse:1}, nf);

    txCol.ensureIndex({'vin.spt': 1}, {}, nf);
    txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var aCol = self.dbconn.collection('archive');
    aCol.ensureIndex({'bhash': 1}, {}, nf);

    var mpCol = self.dbconn.collection('mempool');
    mpCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    mpCol.ensureIndex({'vin.hash': 1, 'vin.n': 1}, {unique: 1, sparse: 1}, nf);
    mpCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var varCol = self.dbconn.collection('var');
    varCol.ensureIndex({'key': 1}, {unique: 1}, nf);

    if(typeof callback == 'function') callback(undefined, self.dbconn);
  });
};

Store.prototype.getBlock = function(hash, callback) {
  var self = this;
  if(!hash) return callback();

  var col = this.dbconn.collection('block');
  //col.find({hash: hash}).toArray(function(err, results) {
  col.findOne({hash: hash}, function(err, block) {
    if(err) {
      return callback(err);
    }
    if(block) {
      block.hash = self.toBuffer(block.hash);
      block.prev_hash = self.toBuffer(block.prev_hash);
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

Store.prototype.setTipBlockHash = function(hash, callback) {
  var col = this.dbconn.collection('var');
  col.update({'key': 'tip'}, {$set: {blockHash: hash}},
	     {upsert: true}, callback);
};

Store.prototype.saveBlock = function(block, callback) {
  var col = this.dbconn.collection('block');
  col.save(block, callback);
};

Store.prototype.updateBlock = function(hash, update, callback) {
  var col = this.dbconn.collection('block');
  col.update({hash: hash}, {$set: update}, callback);
};

Store.prototype.archiveTxs = function(blockHash, callback) {
  var txcol = this.dbconn.collection('tx');
  txcol.find({bhash: blockHash}).toArray(function(err, vals) {
    if(err) return callback(err);
    vals.forEach(function(tx) {
      delete tx._id;      
    });
    var acol = this.dbconn.collection('archive');
    acol.insert(vals, function(err) {
      if(err) return callback(err);
      txcol.remove({bhash: blockHash}, callback);
    });
  });
};

Store.prototype.unArchiveTxs = function(blockHash, callback) {
  var acol = this.dbconn.collection('archive');
  acol.find({bhash: blockHash}).toArray(function(err, vals) {
    if(err) return callback(err);
    var txcol = this.dbconn.collection('tx');
    txcol.insert(vals, function(err) {
      if(err) return callback(err);
      acol.remove({bhash: blockHash}, callback);
    });
  });
};

module.exports.stores = stores;
