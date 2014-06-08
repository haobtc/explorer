var bitcore = require('../alliances/bitcore/bitcore');
var async = require('async');

var mongodb = require('mongodb');
var MongoStore = require('./MongoStore');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');

var hex = function(hex) {return new Buffer(hex, 'hex');};



function Node(netname) {
  this.updateMempool = true;
  this.synchronize = true;

  this.netname = netname;
  this.start_height = -1;
  this.syncTimer = null;
  this.fifoAddBlock = new helper.CallFIFO();
  this.store = MongoStore.stores[netname];
  this.peerman = new PeerManager({
    network: this.netname
  });
  this.peerman.peerDiscovery = true;
}

Node.prototype.insertBlock = function(block, callback) {
  var self = this;
  block.isMain = true;
  var col = this.store.dbconn.collection('block');
  if(typeof callback != 'function') {
    callback = function(err) {if(err){console.error(err)}};
  }
  delete block._id;
  var txes = block.txes;
  delete block.txes;
  var txhashes = txes.map(function(tx) {return tx.hash});

  console.info('save block', block.hash.toString('hex'));
  col.findAndModify(
    {'hash': block.hash}, [],
    {$set: block},
    {upsert: true},
    function(err) {
      if(err) return callback(err);
      if(txes && txes.length > 0) {
	var txcol = self.store.dbconn.collection('tx');

	txcol.insert(txes, function(err) {
	  if(err) {
	    return callback(err);
	  }
	  
	  // Async methods
	  if(self.updateMempool) {
	    var mpcol = self.store.dbconn.collection('mempool');
	    mpcol.remove({hash: {$in: txhashes}}, function(err){});
	  }
	  callback();
	});
      } else {
	callback();
      }
    });
};

Node.prototype.storeTipBlock = function(b, allowReorgs, cb) {
  b.isMain = true;

  if (typeof allowReorgs === 'function') {
    cb = allowReorgs;
    allowReorgs = true;
  }
  if (!b) return cb();

  var self = this;
  var oldTip, oldNext, needReorg = false;
  var newPrev;
  var noBlock = false;

  async.series([
    function(c) {
      self.store.getBlock(b.hash, function(err, val) {
        return c(err ||
		 (val ? new Error('NODE: Ignoring already existing block:' + b.hash.toString('hex')) : null));
      });
    },
    function(c) {
      self.store.dbconn.collection('block').count(function(err, n) {
	if(err) return c(err);
	noBlock = n <= 0;
	c();
      });
    },
    function(c) {
      if(!noBlock) return c();
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      if(b.hash.toString('hex') == helper.reverseBuffer(genesisBlock.hash).toString('hex')) {
	self.insertBlock(b, function(err) {
	  if(err) return c(err);
	  self.store.setTipBlockHash(b.hash, function(err) {
	    c(err || new Error('NODE: Genesis block'));
	  });
	});
      } else c();
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.getBlock(b.prev_hash, function(err, val) {
	if(err) return c(err);
        if (!val) return c('NODE: NEED_SYNC Ignore block with non existing prev: ' + b.hash.toString('hex'));
	newPrev = val;
	b.height = newPrev.height + 1;
	c();
      });
    },
    /*      function(c) {
            self.txDb.createFromBlock(b, function(err) {
            return c(err);
            });
	    }, */
    function(c) {
      if (!allowReorgs) return c();
      self.store.getTipBlock(function(err, val) {
        oldTip = val;
        if (oldTip && newPrev.hash.toString('hex') !== oldTip.hash.toString('hex')) needReorg = true;
        return c();
      });
    },
    function(c) {
      if (!needReorg) return c();
      self.store.getBlock(newPrev.next_hash, function(err, val) {
        if (err) return c(err);
        oldNext = val;
        return c();
      });
    },
    function(c) {
      self.insertBlock(b, c);
    },
    function(c) {
      if (!needReorg) return c();
      console.log('NEW TIP: %s NEED REORG (old tip: %s)', b.hash, oldTip.hash.toString('hex'));
      self.processReorg(oldTip, oldNext, newPrev, c);
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.setTipBlockHash(b.hash, c);
    },
    function(c) {
      newPrev.next_hash = b.hash;
      self.store.saveBlock(newPrev, c);
    }

  ],
	       function(err) {
		 if (err && err.toString().match(/NODE:/)) {
		   console.info(err.toString());
		   err = null;
		 }
		 return cb(err);
	       });
};

Node.prototype.processReorg = function(oldTip, oldNext, newPrev, cb) {
  var self = this;

  var orphanizeFrom;

  async.series([

    function(c) {
      if(!newPrev.isMain) return c();
      orphanizeFrom = oldNext;
      console.log('# Reorg Case 1)');
      c();
    },
    function(c) {
      if (orphanizeFrom) return c();

      console.log('# Reorg Case 2)');
      self.setBranchConnectedBackwards(newPrev, function(err, yBlock, newYBlockNext) {
        if (err) return c(err);
	self.store.getBlock(yBlock.next_hash, function(err, yBlockNext) {
          orphanizeFrom = yBlockNext;
	  yBlock.next_hash = newYBlockNext.hash;
	  self.store.saveBlock(yBlock, function(err) {
            return c(err);
          });
        });
      });
    },
    function(c) {
      if (!orphanizeFrom) return c();
      self.setBranchOrphan(orphanizeFrom, c);
    },
  ],
	       function(err) {
		 return cb(err);
	       });
};

Node.prototype.setBranchOrphan = function(fromBlock, cb) {
  var self = this;
  var pblock = fromBlock.hash;

  async.whilst(
    function() {
      return pblock;
    },
    function(c) {
      pblock.isMain = false;
      self.store.saveBlock(pblock, function(err) {
        if (err) return cb(err);
        self.store.getBlock(pblock.prev_hash, function(err, val) {
	  pblock = val;
          return c(err);
        });
      });
    }, cb);
};

Node.prototype.setBranchConnectedBackwards = function(fromBlock, cb) {
  var self = this;
  var currBlock = fromBlock;
  var lastBlock = fromBlock;
  var isMain;

  async.doWhilst(
    function(c) {
      currBlock.isMain = true;
      self.store.saveBlock(currBlock, function(err) {
        if (err) return c(err);
	self.store.getBlock(currBlock.prev_hash, function(err, val) {
          if (err) return c(err);
	  lastBlock = currBlock;
          currBlock = val;
          isMain = currBlock.isMain;
          return c();
        });
      });
    },
    function() {
      return currBlock && !isMain;
    },
    function(err) {
      console.log('\tFound yBlock:', currBlock);
      return cb(err, currBlock, lastBlock);
    }
  );
};

Node.prototype.run = function(callback) {
  var self = this;
  var p2pPort = bitcore.networks[this.netname].defaultClientPort;
  config[this.netname].peers.forEach(function(peerHost){
    self.peerman.addPeer(new Peer(peerHost, p2pPort));    
  });

  this.peerman.on('connection', function(conn) {
    conn.on('inv', self.handleInv.bind(self));
    conn.on('block', self.handleBlock.bind(self));
    conn.on('tx', self.handleTx.bind(self));
    conn.on('version', self.handleVersion.bind(self));
  });

  this.peerman.on('netConnected', function(info) {
    if(self.synchronize) {
      self.startSync();
    }
    if(typeof callback == 'function') {
      callback();
    }    
  });

  this.peerman.start();
}

Node.prototype.startSync = function() {
  var self = this;
  if(this.syncTimer) {
    return;
  }

  this.syncTimer = setInterval(function() {
    if(!self.fifoAddBlock.isCalling()) {
      self.moreBlocks();
    } else {
    }
  }, 5000);
  setTimeout(function() {
    self.moreBlocks();
  }, 1000);
};

Node.prototype.processTx = function(tx, blockObj) {
  var self = this;
  var txObj = {};
  txObj.hash = helper.reverseBuffer(tx.hash);
  if(blockObj) {
    txObj.bhash = blockObj.hash;
  }
  if(tx.version != 1) {
    txObj.v = tx.version;
  }
  if(tx.lock_time != 0) {
    txObj.lock_time = tx.lock_time;
  }
  txObj.vin = tx.ins.map(function(input, i) {
    var txIn = {};
    var n = input.getOutpointIndex();
    if(n >= 0) {
      txIn.hash = helper.reverseBuffer(new Buffer(input.getOutpointHash()));
      txIn.n = n;
      txIn.k = txIn.hash.toString('hex') + '#' + n;
    }
    txIn.s = input.s;
    return txIn;
  });

  txObj.vout = tx.outs.map(function(out, i) {
    var txOut = {};
    txOut.s = out.s;
    if(tx.outs[i].s) {
      var script = new Script(tx.outs[i].s);
      txOut.addrs = script.getAddrStr(self.netname);
    }
    txOut.v = util.valueToBigInt(out.v).toString();
    return txOut;
  });
  return txObj;
};

Node.prototype.handleBlock = function(info) {
  var self = this;
  var block = info.message.block;

  if(this.fifoAddBlock.fifo.length > 510) {
    return;
  }

  var blockObj = {
    hash: helper.reverseBuffer(block.calcHash()),
    merkle_root: block.merkle_root,
    height: 0,
    nonce: block.nonce,
    version: block.version,
    prev_hash: helper.reverseBuffer(block.prev_hash),
    timestamp: block.timestamp,
    bits: block.bits
  }


  blockObj.txes = block.txs.map(function(tx) {
    return self.processTx(tx, blockObj);
  });

  console.log('** Block Received **', blockObj.hash.toString('hex'), 'at', new Date(blockObj.timestamp * 1000).toString());

  this.fifoAddBlock.unshift(
    this.storeTipBlock.bind(this),
    [blockObj, true], function(err) {
      if(err) {
	console.error(err);
      }
    });
};

Node.prototype.handleTx = function(info) {
  var tx = info.message.tx.getStandardizedObject();
  if(this.updateMempool) {
    var txObj = this.processTx(info.message.tx);
    var col = this.store.dbconn.collection('mempool');
    col.findAndModify({'hash': txObj.hash}, [],
		      {$set: txObj},
		      {upsert: true},
		      function(err, obj) {
			if(err) {
			  console.error(err);
			}
		      });
  }
};

Node.prototype.handleInv = function(info) {
  //    console.log('** Inv **');
  //    console.log(info.message);
  var invs = info.message.invs;
  info.conn.sendGetData(invs);
};

Node.prototype.handleVersion = function(info) {
  //  console.log('** Version **', info.message);
  if(info.message.start_height > this.start_height) {
    this.start_height = info.message.start_height;
  }
};

Node.prototype.moreBlocks = function() {
  var self = this;
  var activeConnections = this.peerman.getActiveConnections();
  if(activeConnections.length == 0) {
    console.warn(this.netname, 'No active connections');
    return;
  }
  var conn = activeConnections[Math.floor(Math.random() * activeConnections.length)];
  if(!conn) {
    return;
  }
  this.store.getTipBlock(function(err, tipBlock) {
    if(tipBlock) {
      var gHash = helper.reverseBuffer(tipBlock.hash);
      conn.sendGetBlocks([gHash], 0);
      console.info('getting blocks starting from', gHash.toString('hex'), 'to', conn.peer.host);
    } else {
      //var gHash = buffertools.fill(new Buffer(32), 0);
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      var inv = {type: 2, hash:genesisBlock.hash};
      conn.sendGetData([inv]);
    }
  });  
};

module.exports = Node;
