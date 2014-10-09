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
    function nf(err) {if(err) {console.error(err);}}

    var blockCol = self.dbConn.collection('block');
    blockCol.ensureIndex({'hash':1},{unique:1}, nf);
    blockCol.ensureIndex({'height':-1}, nf);

    var txCol = self.dbConn.collection('tx');
    txCol.ensureIndex({'hash':1}, {unique:1}, nf);
    txCol.ensureIndex({'binfo.bhash':1}, {}, nf);
    txCol.ensureIndex({'vin.k':1}, {}, nf);
    txCol.ensureIndex({'vout.addrs':1}, {}, nf);
    txCol.ensureIndex({'vout.spent':1}, {}, nf);
    txCol.ensureIndex({'pending': 1}, {}, nf);

    var addrCol = self.dbConn.collection('addr');
    addrCol.ensureIndex({'addr':1},{unique:1}, nf);

    //var txVinCol = self.dbConn.collection('tx_vin');
    //txVinCol.ensureIndex({'key':1},{unique:1}, nf);

    var varCol = self.dbConn.collection('var');
    varCol.ensureIndex({'key': 1}, {unique: 1}, nf);

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
  //console.info('insert block:netname=%s:bhash=%s:height=%d',
  //    this.netname, b.hash.toString('hex'), b.height);
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

Store.prototype.getTipTx = function(cb) {
  var self = this;
  var col = this.dbConn.collection('var');
  col.findOne({key:'tipTx'}, function(err, val) {
    if(err) return cb(err);
    if(!val) {
      var txCol = self.dbConn.collection('tx');
      txCol.find().sort('_id').limit(1).toArray(function(err, txes) {
        if(err) return cb(err);
        if(!txes || txes.length === 0) return cb(undefined, undefined);
        var firstTx = txes[0];
        self.setTipTx(firstTx._id, firstTx.hash, function(err) {
          if(err) return cb(err);
          cb(undefined, {objID:firstTx._id, txHash:firstTx.hash});
        });
      });
    } else {
      cb(undefined, val);
    }
  });
};

Store.prototype.setTipTx = function(objID, hash, cb) {
  var col = this.dbConn.collection('var');
  col.update(
    {key:'tipTx'},
    {$set:{objID:objID, txHash:hash}},
    {upsert:true},
    cb);
};

Store.prototype.getTxListBySinceID = function(sinceID, n, cb) {
  var self = this;
  var col = this.dbConn.collection('tx');
  col.find({_id:{$gt:sinceID}}).sort('_id').limit(n).toArray(function(err, txes) {
    if(err) return cb(err);
    var blockHashList = [];
    txes.forEach(function(tx) {
      tx.hash = helper.toBuffer(tx.hash);
      if(!tx.binfo) return;
      tx.binfo.forEach(function(e) {
        e.bhash = helper.toBuffer(e.bhash);
        blockHashList.push(e.bhash);
      });
    });
    var orphanBlockDict = {};
    var blockCol = self.dbConn.collection('block');
    blockCol.find({hash:{$in:blockHashList}}).toArray(function(err, blocks) {
      if(err) return cb(err);
      blocks.forEach(function(block) {
        if(!block.isMain) orphanBlockDict[helper.toBuffer(block.hash)] = true;
      });
      var resultTxList = [];
      txes.forEach(function(tx) {
        var filter = true;
        if(!tx.binfo) {
          filter = false;
        } else {
          // if there is a block whose isMain true, then tx should not be filtered
          tx.binfo.forEach(function(e) {
            if(!orphanBlockDict[e.bhash]) {
              filter = false;
            }
          });
        }
        if(!filter) resultTxList.push(tx);
      });
      cb(undefined, resultTxList);
    });
  });
};

// tx related
Store.prototype.getTx = function(hash, cb) {
  if(!hash) return cb();
  var col = this.dbConn.collection('tx');
  col.findOne({hash:hash}, function(err, tx) {
    if(err) return cb(err);
    if(tx) {
      tx.hash = helper.toBuffer(tx.hash);
      if(tx.binfo && tx.binfo.length > 0) {
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

Store.prototype.getTxListByVout = function(key, cb) {
  var col = this.dbConn.collection('tx');
  col.find({'vin.k':key}).toArray(function(err, txes) {
    if(err) return cb(err);
    cb(undefined, txes);
  });
};

Store.prototype.insertTx = function(tx, cb) {
  var col = this.dbConn.collection('tx');
  if(tx.bhash) {
    var binfo = {'bhash':tx.bhash, 'bidx':tx.bidx};
    delete tx.bhash;
    delete tx.bidx;
    col.findAndModify(
      {hash:tx.hash},
      [],
      {$addToSet:{'binfo':binfo}, $set:tx},
      {upsert:true,new:false},
      function(err, oldTx) {
        if(err) return cb(err);
        cb(undefined, oldTx);
      });
  } else {
    col.findAndModify(
      {hash:tx.hash},
      [],
      {$set:tx},
      {upsert:true, new:false},
      function(err, oldTx) {
        if(err) return cb(err);
        cb(undefined, oldTx);
      });
  }
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

Store.prototype.updateSpent = function(hash, v, cb) {
  var col = this.dbConn.collection('tx');
  col.findAndModify(
    {hash:hash},
    [],
    v,
    {upsert:false, new:false},
    cb);
};

Store.prototype.getObsoleteTxList = function(cb) {
  var self = this;
  var lastID = new mongodb.ObjectID.createFromTime(Math.floor(new Date().getTime()/1000-86400));
  var obsoleteBlockHashList = [], obsoleteBlockHashDict = [], obsoleteTxList = [];
  async.series([
    function(c) {
      var blockCol = self.dbConn.collection('block');
      blockCol.find({isMain:false, _id:{$lt:lastID}}).toArray(function(err, blocks) {
        if(err) return c(err);
        blocks.forEach(function(b) {
          obsoleteBlockHashList.push(helper.toBuffer(b.hash));
          obsoleteBlockHashDict[helper.toBuffer(b.hash)] = true;
        });
        c();
      });
    },
    function(c) {
      var txCol = self.dbConn.collection('tx');
      txCol.find({'binfo.bhash':{$in:obsoleteBlockHashList}}).toArray(function(err, txes) {
        if(err) return c(err);
        txes.forEach(function(tx) {
          tx.hash = helper.toBuffer(tx.hash);
          tx.binfo.forEach(function(e) {
            e.bhash = helper.toBuffer(e.bhash);
          });
          if(tx.binfo.length === 1) {
            obsoleteTxList.push(tx);
          } else  {
            var filter = false;
            for(var i = 0; i < tx.binfo.length; ++i) {
              var e = tx.binfo[i];
              if(!obsoleteBlockHashDict[e.bhash]) {
                filter = true;
                break;
              }
            }
            if(!filter) obsoleteTxList.push(tx);
          }
        });
        c();
      });
    },
    function(c) {
      var txCol = self.dbConn.collection('tx');
      txCol.find({binfo:{$exists:false}, _id:{$lt:lastID}}).toArray(function(err, txes) {
        if(err) return c(err);
        txes.forEach(function(tx) {
          tx.hash = helper.toBuffer(tx.hash);
          tx.binfo.forEach(function(e) {
            e.bhash = helper.toBuffer(e.bhash);
          });
          obsoleteTxList.push(tx);
        });
        c();
      });
    }
  ],
  function(err){
    if(err) return cb(err);
    cb(undefined, obsoleteBlockHashList, obsoleteTxList);
  });
};

Store.prototype.removeObsoleteTxList = function(txList, cb) {
  var txHashList = txList.map(function(tx) {
    return tx.hash;
  });
  var col = this.dbConn.collection('tx');
  col.remove({hash:{$in:txHashList}}, cb);
};

Store.prototype.removeObsoleteBlockList = function(blockHashList, cb) {
  var col = this.dbConn.collection('block');
  col.remove({hash:{$in:blockHashList}}, cb);
};

Store.prototype.removeTxInBlock = function(bhash, cb) {
  var col = this.dbConn.collection('tx');
  col.remove({'binfo.bhash':bhash}, cb);
};

// addr related
Store.prototype.updateAddrProp = function(addr, v, cb) {
  var col = this.dbConn.collection('addr');
  //console.log('updateAddrProp:addr=%s:v=', addr, v);
  col.findAndModify(
      {addr:addr},
      [],
      {$inc:v},
      {upsert:true, new:true},
      cb);
};
