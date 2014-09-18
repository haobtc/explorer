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
    blk_info : {type:[blkInfoSchema]},
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
      store.connect(function(err, dbConn) {
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
      cb(undefined, self.dbConn);
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
      return cb(undefined, tip.height);
    } else {
      return cb(undefined, self.max_height);
    }
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
  var txes = block.txes;
  delete block.txes;
  console.info('insert block:netname=%s:bhash=%s:height=%d',
      this.netname, b.hash, b.height);
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

// var related
Store.prototype.getTipBlock = function(cb) {
  var self = this;
  var col = this.dbConn.collection('var');
  col.findOne({key:'tip'}, function(err, val) {
    if(err) return cb(err);
    if(val) {
      return self.getBlock(val.blockHash, cb);
    }
    cb();
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

