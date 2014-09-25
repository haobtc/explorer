/*
  'var':
  {
    key : {type:String},
    val : {type:String}
  };
  'block':
  {
    hash : {type:String},
    merkle_root : {type:String},
    height : {type:Number, default:0},
    nonce : {type:String},
    version : {type:Number, default:0},
    prev_hash : {type:String},
    timestamp : {type:Number, default:0},
    bits : {type:Number, default:0},
    next_hash : {type:String},
    main : {type:Boolean, default:false}
  }
  'tx':
  blkInfoSchema = {
    bhash : {type:String},
    bidx : {type:Number, default:-1}
  });

  vinSchema = {
    hash : {type:String},
    n : {type:Number, default:-1},
    s : {type:String},
    q : {type:String},
    addr : {type:String},
    v : {type:String}
  });

  voutSchema = {
    s : {type:String},
    addr : {type:String},
    v : {type:String},
    spent : {type:Number, default:-1}
  });

  txSchema = {
    hash : {type:String},
    binfo : {type:[blkInfoSchema]},
    version : {type:Number, default:0},
    lock_time : {type:Number, default:0},
    vin : {type:[vinSchema]},
    vout : {type:[voutSchema]}
  }
  'addr':
  {
    addr : {type:String},
    balance : {type:String},
    total_received : {type:String},
    total_sent : {type:String}
  }
*/
var mongodb = require('mongodb');
var async = require('async');
var events = require('events');
var helper = require('./helper');
var config = require('./config');

var stores = {};

module.exports.stores = stores;

module.exports.initialize = function(netnames, cb, complete) {
  var tasks = netnames.map(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    return function(c) {
      store.connect(function(err) {
        if(err) return c(err);
        if(typeof cb === 'function') cb(err, netname);
        c(err, netname);
      });
    };
  });
  async.parallel(tasks, complete);
};

function Store(netname) {
  this.netname = netname;
  this.events = new events.EventEmitter();
  this.max_height = -1;
  this.tip_timestamp = 0;
};

Store.prototype.connect = function(cb) {
  var self = this;
  var url = config.networks[this.netname].db.url;
  mongodb.MongoClient.connect(url, function(err, conn) {
    if(err) {
      console.error(err);
      if(typeof cb === 'function') return cb(err);
    }
    self.dbConn = conn;
    // TODO: init indexes
    self.queryMaxHeight(function(err, height) {
      if(err) return cb(err);
      cb(undefined);
    });
  });
};

Store.prototype.queryMaxHeight = function(cb) {
  var self = this;
  this.getTipBlock(function(err, tip) {
    if(err) return cb(err);
    if(tip) {
      self.max_height = tip.height;
      self.tip_timestamp = tip.timestamp;
    }
    cb(undefined, self.max_height);
  });
};

// block related
Store.prototype.getBlock = function(hash, cb) {
  if(!hash) return cb();
  var col = this.dbConn.collection('block');
  col.findOne({hash:hash}, function(err, b) {
    if(err) return cb(err);
    if(b) {
      b.hash = helper.toBuffer(b.hash);
      b.prev_hash = helper.toBuffer(b.prev_hash);
      if(b.next_hash) {
        b.next_hash = helper.toBuffer(b.next_hash);
      }
    }
    cb(undefined, b);
  });
};

Store.prototype.saveBlock = function(b, cb) {
  this.dbConn.collection('block').save(b, cb);
};

Store.prototype.insertBlock = function(b, cb) {
  if(!b.isMain) {
    console.warn('insert block:main should be true:netname=%s:bhash=%s',
        this.netname, b.hash.toString('hex'));
    b.isMain = true;
  }
  if(typeof cb !== 'function') {
    cb = function(err) {
      if(err) console.error(err.stack);
    };
  }
  if(b._id) {
    console.warn('insert block:_id should not exist:netname=%s:bhash=%s',
      this.netname, b.hash.toString('hex'));
    delete b._id;
  }
  var txes = b.txes;
  delete b.txes;
  console.info('insert block:netname=%s:bhash=%s:height=%d',
      this.netname, b.hash.toString('hex'), b.height);
  var col = this.dbConn.collection('block');
  col.findAndModify(
      {hash:b.hash},
      [],
      {$set:b},
      {upsert:true,new:true},
      function(err, b) {
        if(err) return cb(err);
        cb(undefined, txes);
      });
};

Store.prototype.updateBlock = function(b, cb) {
  var col = this.dbConn.collection('block');
  col.findAndModify(
      {hash:b.hash},
      [],
      {upsert:false,new:false},
      function(err, val) {
        if(err) return cb(err);
        cb(err,val);
      });
};

Store.prototype.getBlockCount = function(cb) {
  var col = this.dbConn.collection('block');
  col.count(cb);
};

// var related
Store.prototype.getTipBlock = function(cb) {
  var self = this;
  var col = this.dbConn.collection('var');
  col.findOne({key:'tip'}, function(err, val) {
    if(err) return cb(err);
    if(val) {
      self.getBlock(val.blockHash, cb);
    } else {
      console.log('no tip');
      cb(undefined, undefined);
    }
  });
};

Store.prototype.setTipBlock = function(b, cb) {
  var col = this.dbConn.collection('var');
  col.update(
      {key:'tip'},
      {$set : {blockHash:b.hash}},
      {upsert:true},
      cb);
};

Store.prototype.getPeerList = function(netname, cb) {
  var col = this.dbConn.collection('var');
  col.findOne({key:'peers.'+netname}, cb);
};

Store.prototype.savePeerList = function(netname, peerList, cb) {
  var col = this.dbConn.collection('var');
  col.findAndModify(
      {key:'peers.'+netname},
      [],
      {$set:{peers:peerList}},
      {upsert:true,new:true},
      cb);
};

// tx related
Store.prototype.getTx = function(hash, cb) {
  if(!hash) return cb();
  var col = this.dbConn.collection('tx');
  col.findOne({hash:hash}, function(err, tx) {
    if(err) return cb(err);
    if(tx) {
      tx.hash = helper.toBuffer(tx.hash);
      if(!tx.binfo && tx.binfo.length > 0) {
        tx.binfo.forEach(function(entry) {
          entry.bhash = helper.toBuffer(entry.bhash);
        });
      }
    }
    cb(undefined, tx);
  });
};

Store.prototype.getPendingTx = function(cb) {
  var col = this.dbConn.collection('tx');
  col.findAndModify(
      {pending:true},
      [],
      {$unset:{pending:''}},
      {},
      function(err, tx) {
        if(err) return cb(err);
        cb(err, tx);
      });
};

Store.prototype.getRawTxList = function(cb) {
  var col = this.dbConn.collection('tx');
  col.find({binfo:{$exists:false},raw:{$exists:true}}).toArray(cb);
};

Store.prototype.getTxList = function(hashList, cb) {
  var col = this.dbConn.collection('tx');
  col.find({hash:{$in:hashList}}).toArray(function(err, arr) {
    if(err) return cb(err);
    cb(undefined, arr);
  });
};

Store.prototype.getTxListByBlock = function(blockHashList, cb) {
  var col = this.dbConn.collection('tx');
  col.find({'binfo.bhash':{$in:blockHashList}}).toArray(function(err, arr) {
    if(err) return cb(err);
    cb(undefined, arr);
  });
};

Store.prototype.getMempoolTxList = function(txHashList, cb) {
  var col = this.dbConn.collection('tx');
  col.find({hash:{$in:txHashList}}).toArray(function(err, txes) {
    if(err) return cb(err);
    txes.forEach(function(tx) {
      tx.hash = helper.toBuffer(tx.hash);
      if(tx.raw) tx.raw = helper.toBuffer(tx.raw);
      if(tx.binfo) console.warn('tx is not mempool:hash=%s',
        tx.hash.toString('hex'));
    });
    cb(err, txes);
  });
};

Store.prototype.insertTx = function(tx, cb) {
  var col = this.dbConn.collection('tx');
  col.insert(tx,{w:1},function(err) {
    if(err) return cb(err);
    cb(undefined);
  });
};

Store.prototype.updateTx = function(tx, cb) {
  var col = this.dbConn.collection('tx');
  col.findAndModify(
      {hash:tx.hash},
      [],
      {$set:tx},
      {upsert:false,new:true},
      function(err, newTx) {
        if(err) return cb(err);
        cb(undefined, newTx);
      });
};

Store.prototype.updatePartialTx = function(hash, v, cb) {
  var col = this.dbConn.collection('tx');
  col.findAndModify(
      {hash:hash},
      [],
      {$set:v},
      {upsert:false,new:false},
      cb);
};

Store.prototype.updateSpent = function(hash, spentInfo, cb) {
  var col = this.dbConn.collection('tx');
  col.findAndModify(
      {hash:hash},
      [],
      {$inc:spentInfo},
      {upsert:false, new:false},
      cb);
};

Store.prototype.removeObsoleteTxList = function(cb) {
  var lastID = new mongodb.ObjectID.createFromTime(
      Math.floor(new Date().getTime()/1000-86400));
  var col = this.dbConn.collection('tx');
  var cond = {_id:{$lt:lastID}};
  var removeTxList;
  col.find(cond).toArray(function(err, txList) {
    if(err) return cb(err);
    removeTxList = txList;
    col.remove(cond, function(err) {
      if(err) return cb(err);
      cb(undefined, removeTxList);
    });
  });
};

Store.prototype.removeTxInBlock = function(bhash, cb) {
  var col = this.dbConn.collection('tx');
  col.remove({'binfo.bhash':bhash}, cb);
};

// addr related
Store.prototype.updateAddrProp = function(addr, v, cb) {
  var col = this.dbConn.collection('addr');
  console.log('updateAddrProp:addr=%s:v=', addr, v);
  col.findAndModify(
      {addr:addr},
      [],
      {$inc:v},
      {upsert:true, new:true},
      cb);
};
