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
var Skip = helper.Skip;

var hex = function(hex) {return new Buffer(hex, 'hex');};

function Node(netname) {
  var self = this;
  this.updateMempool = true;
  this.synchronize = true;

  this.netname = netname;
  this.start_height = -1;
  this.syncTimer = null;
  this.allowOldBlock = true;
  this.fifoAddBlock = new helper.CallFIFO();

  this.peerman = new PeerManager({
    network: this.netname
  });
  this.peerman.peerDiscovery = true;

  this.store = MongoStore.stores[netname];
  this.store.on('added txes', function(txes) {
    if(self.updateMempool) {
      var mpcol = self.store.dbconn.collection('mempool');
      var txhashes = txes.map(function(tx) {return tx.hash});
      mpcol.remove({hash: {$in: txhashes}}, function(err){});
    }
    self.updateSpent(txes);
  });
  this.store.on('removed txes', function(txes) {
    self.updateUnSpent(txes);
  });
}

Node.prototype.insertBlock = function(block, callback) {
  var self = this;
  block.isMain = true;
  var col = this.store.dbconn.collection('block');
  if(typeof callback != 'function') {
    callback = function(err) {if(err){console.error(err.stack)}};
  }
  delete block._id;
  var txes = block.txes;
  delete block.txes;

  console.info('save block', this.netname, block.hash.toString('hex'), 'height=', block.height);
  col.findAndModify(
    {'hash': block.hash}, [],
    {$set: block},
    {upsert: true, new: true},
    function(err, b) {
      if(err) return callback(err);
      if(txes && txes.length > 0) {
	self.store.addTxes(txes, callback);
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
		 (val? new Skip('Ignoreing already existing block:' + b.hash.toString('hex')): null));
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
	  self.store.setTipBlock(b, function(err) {
	    c(err || new Skip('Genesis block'));
	  });
	});
      } else c();
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.getBlock(b.prev_hash, function(err, val) {
	if(err) return c(err);
        if(!val) return c(new Skip('Ignore block with non existing prev: ' + b.hash.toString('hex')));
	/*if(val.height < self.store.max_height - 1000) 
	  return c(new Skip('Prev block hash ', val.height, 'is too early, dont revoke!')); */

	newPrev = val;
	b.height = newPrev.height + 1;
	self.store.max_height = b.height;
	self.store.tip_timestamp = b.timestamp;
	c();
      });
    },
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
      console.log('NEW TIP: %s NEED REORG (old tip: %s)', b.hash.toString('hex'), oldTip.hash.toString('hex'));
      self.processReorg(oldTip, oldNext, newPrev, c);
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.setTipBlock(b, c);
    },
    function(c) {
      newPrev.next_hash = b.hash;
      self.store.saveBlock(newPrev, c);
    }

  ],
	       function(err) {
		 if(err && err instanceof Skip) {
		   //console.info('SKIP', err.message);
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
	  self.store.saveBlock(yBlock, c);
        });
      });
    },
    function(c) {
      if (!orphanizeFrom) return c();
      self.setBranchOrphan(orphanizeFrom, c);
    },
  ], cb);

};

Node.prototype.setBranchOrphan = function(fromBlock, cb) {
  var self = this;
  var pblock = fromBlock;

  async.whilst(
    function() {
      return pblock;
    },
    function(c) {
      console.info('set orphan', pblock.hash.toString('hex'));
      self.setBlockMain(pblock, false, function(err) {
        if (err) return c(err);
        self.store.getBlock(pblock.next_hash, function(err, val) {
	  if(err) return c(err);
	  pblock = val;
          return c(err);
        });
      });
    }, cb);
};

Node.prototype.setBlockMain = function(block, isMain, callback) {
  var self = this;
  var changed = isMain !== block.isMain;
  block.isMain = isMain;
  this.store.saveBlock(block, function(err) {
    if(err) return callback(err);
    if(changed) {
      if(block.isMain) {
	self.unArchiveTxes(block.hash, callback);
      } else {
	self.archiveTxes(block.hash, callback);
      }
    } else {
      callback(err);
    }
  });
};

Node.prototype.archiveTxes = function(blockHash, callback) {
  var self = this;
  var txcol = this.store.dbconn.collection('tx');
  txcol.find({bhash: blockHash}).toArray(function(err, vals) {
    if(err) return callback(err);
    if(vals.length <= 0) return callback();
    vals.forEach(function(tx) {
      delete tx._id;      
    });
    self.store.emit('removed txes', vals);
    var acol = self.store.dbconn.collection('archive');
    acol.insert(vals, function(err) {
      if(err) return callback(err);
      txcol.remove({bhash: blockHash}, callback);
    });
  });
};

Node.prototype.unArchiveTxes = function(blockHash, callback) {
  var self = this;
  var acol = this.store.dbconn.collection('archive');
  acol.find({bhash: blockHash}).toArray(function(err, vals) {
    if(err) return callback(err);
    if(vals.length <= 0) return callback();
    vals.forEach(function(tx) {
      delete tx._id;    
    });
    self.store.addTxes(vals, function(err) {
      if(err) return callback(err);
      acol.remove({bhash: blockHash}, callback);
    });
  });
};

Node.prototype.setBranchConnectedBackwards = function(fromBlock, cb) {
  var self = this;
  var currBlock = fromBlock;
  var lastBlock = fromBlock;
  var isMain;

  async.doWhilst(
    function(c) {
      self.setBlockMain(currBlock, true, function(err) {
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
  this.store.getVar('peers.' + this.netname, function(err, v) {
    if(err) return callback(err);
    var peers = config.networks[self.netname].peers;
    if(v && v.peers && v.peers.length > 0) {
      peers = v.peers;
    }

    var p2pPort = bitcore.networks[self.netname].defaultClientPort;
    peers.forEach(function(peerHost){
      if(peerHost.indexOf(':') < 0) {
	peerHost = peerHost + ':' + p2pPort;
      }
      self.peerman.addPeer(new Peer(peerHost));
    });

    self.peerman.on('connection', function(conn) {
      conn.on('inv', self.handleInv.bind(self));
      conn.on('block', self.handleBlock.bind(self));
      conn.on('tx', self.handleTx.bind(self));
      conn.on('getdata', self.handleGetData.bind(self));
      conn.on('version', self.handleVersion.bind(self));

      if(Math.random() < 0.2) {
	// Don't have to save peers every time
	self.savePeers();
      }
    });

    self.peerman.on('netConnected', function(info) {
      if(self.synchronize) {
	self.startSync();
      }
      if(typeof callback == 'function') {
	callback();
      }
    });
    self.peerman.start();
  });
};

Node.prototype.savePeers = function(callback) {
  var peers = this.peerman.getActiveConnections().slice(0, 10).map(function(conn) {
    return conn.peer.host + ':' + conn.peer.port;
  });
  this.store.saveVar({
    key: 'peers.' + this.netname,
    peers: peers
  }, callback||function(err){if(err) throw err;});
};

Node.prototype.startSync = function() {
  var self = this;
  if(this.syncTimer) {
    return;
  }

  this.syncTimer = setInterval(function() {
    if(!self.fifoAddBlock.isCalling()) {
      self.moreBlocks();
    } else {
      console.info('queue length', self.netname, self.fifoAddBlock.fifo.length);
    }
  }, 9000 + 2000 * Math.random());

  this.mempoolTimer = setInterval(this.processMempool.bind(this), 30 * 1000);

  setTimeout(function() {
    self.moreBlocks();
    self.processMempool();
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
    txIn.q = input.q;
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

  if(!this.allowOldBlock  &&
     block.timestamp < this.store.tip_timestamp - 86400) {
    //console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return;
  }

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

  console.log('** Block Received **', this.netname, blockObj.hash.toString('hex'), 'at', new Date(blockObj.timestamp * 1000).toString());

  this.fifoAddBlock.unshift(
    this.storeTipBlock.bind(this),
    [blockObj, true], function(err) {
      if(err) {
	throw err;
      }
    });
};

Node.prototype.handleTx = function(info) {
  var self = this;
  //  var tx = info.message.tx.getStandardizedObject();
  if(this.updateMempool) {
    var txObj = this.processTx(info.message.tx);
    this.store.getTx(txObj.hash, true, function(err, obj) {
      if(err) throw err;
      if(obj) return; // Already have a tx in block chain
      var col = self.store.dbconn.collection('mempool');
      col.findAndModify({'hash': txObj.hash}, [],
			{$set: txObj},
			{upsert: true},
			function(err, obj) {
			  if(err) throw err;
			});
    });
  }
};

Node.prototype.handleInv = function(info) {
  //    console.log('** Inv **');
  //    console.log(info.message);
  var invs = info.message.invs;
  info.conn.sendGetData(invs);
};

Node.prototype.handleGetData = function(info) {
  var txHashList = [];
  info.message.invs.forEach(function(inv) {
    if(inv.type == 1) {
      txHashList.push(helper.reverseBuffer(inv.hash));
    }
  });
  if(txHashList.length > 0) {
    this.store.getMempoolTxes(txHashList, function(err, txes) {
      if(err) throw err;
      txes.forEach(function(tx) {
	if(tx.raw) {
	  info.conn.sendMessage('tx', tx.raw);
	}
      });
    });
  }
};

Node.prototype.handleVersion = function(info) {
  //  console.log('** Version **', info.message);
  if(info.message.start_height > this.start_height) {
    this.start_height = info.message.start_height;
  }
};


Node.prototype.randomConn = function() {
  var activeConnections = this.peerman.getActiveConnections();
  if(activeConnections.length == 0) {
    console.warn(this.netname, 'No active connections');
    return;
  }
  var conn = activeConnections[Math.floor(Math.random() * activeConnections.length)];
  return conn;
};

Node.prototype.moreBlocks = function() {
  var self = this;

  var conn = this.randomConn();
  if(!conn) {
    console.warn(this.netname, 'No active connections');
    return;
  }
  this.store.getTipBlock(function(err, tipBlock) {
    if(tipBlock) {
      var gHash = helper.reverseBuffer(tipBlock.hash);
      conn.sendGetBlocks([gHash], 0);
      console.info(self.netname, 'getting blocks starting from', gHash.toString('hex'), 'to', conn.peer.host);
      if(tipBlock.prev_hash && Math.random() < 0.5) {
	var conn1 = self.randomConn();
	if(!!conn1) {
	  var pHash = helper.reverseBuffer(tipBlock.prev_hash);;
	  conn1.sendGetBlocks([pHash], 0);
	  console.info(self.netname, 'getting blocks starting from prev ', pHash.toString('hex'), 'to', conn1.peer.host);
	}
      }
    } else {
      //var gHash = buffertools.fill(new Buffer(32), 0);
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      var inv = {type: 2, hash:genesisBlock.hash};
      conn.sendGetData([inv]);
    }
  });  
};

Node.prototype.loopBlocks = function(key, times, fn, complete) {
  var self = this;

  var blockCol = this.store.dbconn.collection('block');
  var txCol = this.store.dbconn.collection('tx');
  var varCol = this.store.dbconn.collection('var');

  var currBlock;
  async.timesSeries(times, function(n, c) {
    var iterator;
    async.series(
      [function(c) {
	if(!!currBlock) return c();
	varCol.findOne({key: key}, function(err, v) {
	  if(err) return c(err);
	  iterator = v;
	  if(v && v.blockHash == 'No block') {
	    return c(new Skip('No block'));
	  }
	  c();
	});
      },
       function(c) {
	 if(!!iterator) return c();
	 if(!!currBlock) {
	   iterator = {key: key, blockHash: currBlock.hash};
	   return c();
	 }
	 blockCol.findOne(function(err, block) {
	   if(err) return c(err);
	   if(!block) return c(new Skip('No block!'));
	   currBlock = block;
	   iterator = {'key': key, blockHash: block.hash};
	   c();
	 });
       },
       function(c) {
	 if(!!currBlock) return c();
	 self.store.getBlock(iterator.blockHash, function(err, block){
	   if(err) return c(err);
	   if(!block) return c(new Skip('No block!'));
	   currBlock = block;
	   c();
	 });
       },
       function(c) {
	 fn(currBlock, c);
       },
       function(c) {
	 self.store.getBlock(currBlock.next_hash, function(err, block) {
	   if(err) return c(err);
	   currBlock = block;
	   c();
	 });
       },
       function(c) {
	 if(currBlock) {
	   varCol.update({key: key}, {$set: {blockHash: currBlock.hash}}, {upsert: true}, c);
	 } else {
	   varCol.update({key: key}, {$set: {blockHash: 'No block'}}, {upsert: true}, c);
	 }
       }
      ],
      function(err) {
	if(err && err instanceof Skip) {
	  err = null;
	}
	if(err) {
	  throw err;
	}
	c();
      });
  }, function(err) {
    complete(err);
  });
};

Node.prototype.sendTx = function(tx, callback) {
  var self = this;
  var txObj = self.processTx(tx);

  function deliverToSiblings() {
    var conns = self.peerman.getActiveConnections();
    if(conns.length == 0) {
      return {code:-1, message:'no connection to send tx'};
    }
    conns.forEach(function(conn) {
      conn.sendTx(tx);
    });
    return txObj.hash.toString('hex');
  }

  this.store.getTx(txObj.hash, true, function(err, obj) {
    if(err) return callback(err, false);
    if(obj) return callback(undefined,
			    {code:-5, message: 'transaction already in block chain.'});

    var col = self.store.dbconn.collection('mempool');
    txObj.raw = tx.serialize();
    col.findAndModify({'hash': txObj.hash}, [],
		      {$set: txObj},
		      {upsert: true},
		      function(err, obj) {
			if(err) callback(err, false);
			callback(undefined, txObj.hash.toStrinng('hex'));
			deliverToSiblings();
		      });
  });
};

Node.prototype.processMempool = function(tx, callback) {
  var self = this;
  var col = self.store.dbconn.collection('mempool');
  var removableTxIDs = [];
  var invs = [];
  var rawTXes = [];

  col.find().limit(2000).toArray(function(err, txes) {
    txes.forEach(function(tx) {
      tx.hash = helper.toBuffer(tx.hash);
      if(tx._id.getTimestamp().getTime() < new Date().getTime() - 86400 * 1000 * 7) {
	removableTxIDs.push(tx._id);
      } else if(!!tx.raw){
	invs.push({
	  type: 1,
	  hash: helper.reverseBuffer(tx.hash)
	});
	rawTXes.push(helper.toBuffer(tx.raw));
      }
    });
    async.series([
      function(c) {
	if(removableTxIDs.length <= 0) return c();
	col.remove({_id: removableTxIDs}, c);
      },
      function(c) {
	if(invs.length <= 0)  return c();

	console.info('send raw invs', invs);
	var conns = self.peerman.getActiveConnections();
	conns.forEach(function(conn) {
	  if(Math.random() < 0.5) {
	    conn.sendRawInv(invs);
	  } else {
	    rawTXes.forEach(function(raw) {
	      conn.sendMessage('tx', raw);
	    });
	  }
	});
      }
    ], callback);
  });
};

// Extra tasks on modification of txes
Node.prototype.updateSpent = function(txes) {
  var col = this.store.dbconn.collection('tx');
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	var setV = {};
	setV['vout.' + input.n + '.spt'] = true;
	col.update({'hash': input.hash}, {$set:setV}, function(err) {
	  if(err) throw err;
	});
      }
    });
  });
};

Node.prototype.updateUnSpent = function(txes) {
  var col = this.store.dbconn.collection('tx');
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	var setV = {};
	setV['vout.' + input.n + '.spt'] = false;
	col.update({'hash': input.hash}, {$unset:setV}, function(err) {
	  if(err) throw err;
	});
      }
    });
  });
};


module.exports = Node;
