var bitcore = require('../alliances/bitcore/bitcore');
var getenv = require('getenv');
var async = require('async');
var mongodb = require('mongodb');
var MongoStore = require('./MongoStore');
var BlockReader = require('./BlockReader');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');
var Queue = require('./queue');
var Consts = rquire('./consts');
var Skip = helper.Skip;

var hex = function(hex) {return new Buffer(hex, 'hex');};

function Node(netname) {
  var self = this;
  this.updateMempool = true;
  this.synchronize = true;
  this.syncMethod = 'net';
  this.addBlocks = true;

  this.netname = netname;
  this.start_height = -1;
  this.syncTimer = null;
  this.allowOldBlock = true;
  this.blockQueue = new Queue({timeout:60000, limit:1810});

  this.pendingBlockHashes = {};

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
    //self.updateSpent(txes);
  });
  this.store.on('removed txes', function(txes) {
    //self.updateUnSpent(txes);
  });
}

Node.prototype.updateBlock = function(block, callback) {
  var self = this;
  var col = this.store.dbconn.collection('block');
  var txcol = this.store.dbconn.collection('tx');
  var txes = block.txes;

  delete block.height;
  delete block._id;
  delete block.txes;

  async.series([
    function(c) {
      
      col.findAndModify(
	{hash:block.hash}, [],
	{$set: block},
	{upsert: false, 'new': false},
	function(err, b) {
	  if(err) return c(err);
	  if(!b) return c(new Skip('block not found'));
	  c();
	});
    },
    function(c) {
      txcol.remove({bhash: block.hash}, c);
    },
    function(c) {
      var txarr = [];
      txarr = txes.map(function(tx) { return tx.hash});
      txcol.find({hash: {$in: txarr}}).toArray(function(err, arr) {
	if(err) return c(err);
	//if(!!arr && arr.length > 0)
	arr.forEach(function(t) {
	  console.info('yyy', t.hash.toString('hex'), t.bhash.toString('hex'));
	  });
	c();
      });
    },
    function(c) {
      txcol.insert(txes, function(err) {
	c(err);
      });
    }
  ], function(err) {
    if(err instanceof Skip) {
      err = undefined;
    }
    callback(err);
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
  var start = new Date();
  var sumtime = 0;
  var tss = [];
  function pushts(label) {
    var t = (new Date() - start)/1000;
    sumtime += t;
    tss.push(label + '-' + t);
    start = new Date();
  }

  async.series([
    function(c) {
      self.store.getBlock(b.hash, function(err, val) {
	pushts('getblock');
	return c(err ||
		 (val? new Skip('Ignore already existing block:' + b.hash.toString('hex')): null));
      });
    },
    function(c) {
      self.store.dbconn.collection('block').count(function(err, n) {
	pushts('getcount');
	if(err) return c(err);
	noBlock = n <= 0;
	c();
      });
    },
    function(c) {
      if(!noBlock) return c();
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      if(b.hash.toString('hex') == helper.reverseBuffer(genesisBlock.hash).toString('hex')) {
	self.addNewBlock(b, function(err) {
	  pushts('addNewBlock');
	  if(err) return c(err);
	  self.store.setTipBlock(b, function(err) {
	    pushts('settip');
	    c(err || new Skip('Genesis block'));
	  });
	});
      } else c();
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.getBlock(b.prev_hash, function(err, val) {
	pushts('getprevblock');
	if(err) return c(err);
        if(!val) {
	  if(b.retry == undefined) {
	    b.retry = 0;
	  }
	  if(b.retry > 0) {
	    b.retry--;
	    self.blockQueue.task({timestamp: b.timestamp,
				  hexHash:b.hash.toString('hex')}, function(c) {
	      self.storeTipBlock(b, true, function(err) {
		c();
	      });    
	    }, {hash: b.hash.toString('hex')});
	    return c(new Skip('retry block ' + b.hash.toString('hex')));
	  } else {
	    delete b.retry;
	    return c(new Skip('Ignore block due to non existing prev: ' + b.hash.toString('hex')));
	  }
	} else {
	  if(b.retry) {
	    console.info('retry block', b.hash.toString('hex'));
	    delete b.retry;
	   }
	}
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
	pushts('gettipblock');
	if(err) return c(err);
        oldTip = val;
        if (oldTip && newPrev.hash.toString('hex') !== oldTip.hash.toString('hex')) needReorg = true;
        return c();
      });
    },
    function(c) {
      if (!needReorg) return c();
      self.store.getBlock(newPrev.next_hash, function(err, val) {
	pushts('getblockprevnext');
        if (err) return c(err);
        oldNext = val;
        return c();
      });
    },
    function(c) {
      self.addNewBlock(b, function(err) {
	pushts('addNewBlock');
	c(err);
      });
    },
    function(c) {
      if (!needReorg) return c();
      console.log('NEW TIP: %s NEED REORG (old tip: %s)', b.hash.toString('hex'), oldTip.hash.toString('hex'));
      self.processReorg(oldTip, oldNext, newPrev, function(err) {
	pushts('processReorg');
	c(err);
      });
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.setTipBlock(b, function(err) {
	pushts('settipblock');
	c(err);
      });
    },
    function(c) {
      newPrev.next_hash = b.hash;
      self.store.saveBlock(newPrev, function(err) {
	pushts('saveblock');
	c();
      });
    }
  ],
	       function(err) {
		 if(sumtime > 10.0) {
		   console.info('storetipblock using', tss);
		 }
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
  var unArchivedBlocks = [];
  var archivedBlocks = [];

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
      self.setBlocksUnOrphan(newPrev, function(err, yBlock, newYBlockNext, arr) {
        if (err) return c(err);
	unArchivedBlocks = arr;
	self.store.getBlock(yBlock.next_hash, function(err, yBlockNext) {
          orphanizeFrom = yBlockNext;
	  yBlock.next_hash = newYBlockNext.hash;
	  self.store.saveBlock(yBlock, c);
        });
      });
    },
    function(c) {
      if (!orphanizeFrom) return c();
      self.setBranchOrphan(orphanizeFrom, function(err, arr) {
	if(err) c(err);
	archivedBlocks = arr;
	c();
      });
    },
    function(c) {
      self.archiveTxes(archivedBlocks, c);
    },
    function(c) {
      self.unArchiveTxes(unArchivedBlocks, c);
    }
  ], cb);
};

Node.prototype.setBranchOrphan = function(fromBlock, cb) {
  var self = this;
  var pblock = fromBlock;
  var blockActions = [];

  async.whilst(
    function() {
      return pblock;
    },
    function(c) {
      console.info('set orphan', pblock.hash.toString('hex'));
      self.setBlockMain(pblock, false, function(err, changed) {
        if (err) return c(err);
	if(changed)
	  blockActions.push(pblock.hash);
        self.store.getBlock(pblock.next_hash, function(err, val) {
	  if(err) return c(err);
	  pblock = val;
          return c(err);
        });
      });
    }, function(err) {
      cb(err, blockActions);
    });
};

Node.prototype.setBlockMain = function(block, isMain, callback) {
  var self = this;
  var changed = isMain !== block.isMain;
  block.isMain = isMain;
  this.store.saveBlock(block, function(err) {
    if(err) return callback(err);
    callback(err, changed);
  });
};

Node.prototype.archiveTxes = function(blockHashList, callback) {
  if(!blockHashList || blockHashList.length == 0)
    return callback();

  console.info('archiveTxes', blockHashList);
  var self = this;
  var txcol = this.store.dbconn.collection('tx');
  txcol.find({bhash: {$in: blockHashList}}).toArray(function(err, vals) {
    if(err) return callback(err);
    if(vals.length <= 0) return callback();
    vals.forEach(function(tx) {
      delete tx._id;      
    });
    self.store.emit('removed txes', vals);
    var acol = self.store.dbconn.collection('archive');
    acol.insert(vals, function(err) {
      if(err) return callback(err);
      txcol.remove({bhash: {$in: blockHashList}}, callback);
    });
  });
};

Node.prototype.unArchiveTxes = function(blockHashList, callback) {
  if(!blockHashList || blockHashList.length == 0)
    return callback();
  console.info('unArchiveTxes', blockHashList);
  var self = this;
  var acol = this.store.dbconn.collection('archive');
  acol.find({bhash: {$in: blockHashList}}).toArray(function(err, vals) {
    if(err) return callback(err);
    if(vals.length <= 0) return callback();
    vals.forEach(function(tx) {
      delete tx._id;    
    });
    self.store.addTxes(vals, function(err) {
      if(err) return callback(err);
      acol.remove({bhash: {$in: blockHashList}}, callback);
    });
  });
};

Node.prototype.setBlocksUnOrphan = function(fromBlock, cb) {
  var self = this;
  var currBlock = fromBlock;
  var lastBlock = fromBlock;
  var isMain;
  var blockActions = [];

  async.doWhilst(
    function(c) {
      self.setBlockMain(currBlock, true, function(err, changed) {
        if (err) return c(err);
	if(changed)
	  blockActions.push(currBlock.hash);
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
      //console.log('\tFound yBlock:', currBlock);
      return cb(err, currBlock, lastBlock, blockActions);
    }
  );
};

Node.prototype.run = function(callback) {
  var self = this;
  this.store.getPeerList('peers.' + this.netname, function(err, v) {
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
      console.info('netConnected');
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
  this.store.savePeerList(this.netname, peers,
      callback||function(err){if(err) throw err;});
};

Node.prototype.startSync = function() {
  var self = this;
  if(this.syncTimer) {
    return;
  }

  this.syncTimer = setInterval(function() {
    console.info('queue length', self.netname, self.blockQueue.size());
    if(self.blockQueue.size() < 5) {
      self.requireBlocks();
    } else {
      console.info('queue length', self.netname, self.blockQueue.size());
    }
  }, 4000 + 2000 * Math.random());

  this.mempoolTimer = setInterval(this.processMempool.bind(this), 30 * 1000);

  this.pendingTimer = setInterval(function() {
    self.sendPendingTX(function(err) {
      if(err) throw err;
    });
  }, 3000);

  setTimeout(function() {
    self.requireBlocks();
    self.processMempool();
  }, 1000);
};

Node.prototype.processTx = function(tx, idx, blockObj) {
  return helper.processTx(this.netname, tx, idx, blockObj);
};

Node.prototype.handleBlock = function(info) {
  var block = info.message.block;
  this.onBlock(block, function(err, q) {
    if(err) throw err;
  });
};

Node.prototype.onBlock = function(block, cb) {
  var self = this;
  var pendingFunc = this.pendingBlockHashes[block.calcHash(bitcore.networks[self.netname].blockHashFunc).toString('hex')];
  if(!pendingFunc) {
    if(!this.allowOldBlock &&
       this.store.tip_timestamp > 0 &&
       block.timestamp < this.store.tip_timestamp - 86400 * 10) {
      console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
      return cb(null, false);
    }
    if(!this.allowOldBlock &&
       this.store.tip_timestamp > 0 &&
       block.timestamp > this.store.tip_timestamp + 86400 * 20) {
      console.info('deny new block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
      return cb(null, false);
    }

    if(this.blockQueue.isFull()) {
      console.info('block is full');
      return cb(null, false);
    }
  }
  var blockObj = {
    hash: helper.reverseBuffer(block.calcHash(bitcore.networks[self.netname].blockHashFunc)),
    merkle_root: block.merkle_root,
    height: 0,
    nonce: block.nonce,
    version: block.version,
    prev_hash: helper.reverseBuffer(block.prev_hash),
    timestamp: block.timestamp,
    bits: block.bits
  };

  blockObj.txes = block.txs.map(function(tx, idx) {
    return self.processTx(tx, idx, blockObj);
  });

  //console.log('** Block Received **', self.netname, blockObj.hash.toString('hex'), 'at', new Date(blockObj.timestamp * 1000).toString());
  if(pendingFunc) {
    pendingFunc(blockObj);
    delete this.pendingBlockHashes[block.calcHash(bitcore.networks[self.netname].blockHashFunc).toString('hex')];
  }
  if(this.addBlocks) {
    var queued = self.enqueueBlock(blockObj);
    return cb(null, queued);
  } else {
    return cb(null, false);
  }
};

Node.prototype.stop = function(cb) {
  var self = this;
  this.blockQueue.task({timestamp: 0,
			hexHash: ''},
		       function(c){
			 self.blockQueue.closed = true;
			 cb();
			 c();
		       });
};

Node.prototype.enqueueBlock = function(blockObj, blockContinue) {
  var self = this;
  return this.blockQueue.task({timestamp:blockObj.timestamp,
			       hexHash:blockObj.hash.toString('hex')},
			      function(c) {
				self.storeTipBlock(blockObj, true, function(err) {
				  if(err) {
				    console.error(err);
				  }
				  c();
				});    
			      }, {hash: blockObj.hash.toString('hex')});
};

Node.prototype.handleTx = function(info) {
  var self = this;
  //  var tx = info.message.tx.getStandardizedObject();
  if(this.updateMempool) {
    var txObj = this.processTx(info.message.tx, -1);
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

Node.prototype.requireBlocks = function() {
  if(this.syncMethod == 'file') {
    return this.requireBlocksFromFile();
  } else {
    return this.requireBlocksFromNet();
  }
};

Node.prototype.getIndexedBlocks = function(query, n, callback) {
  var self = this;
  var bindex = this;
  async.series([
    function(c) {
      var col = self.store.dbconn.collection('blockindex');
      col.findOne(query, function(err, val){
	if(err) return c(err);
	bindex = val;
	c();
      });
    },
    function(c) {
      if(!bindex) return c();
      if(!self.blockReader) {
	self.blockReader = new BlockReader(getenv('HOME') + '/.' + self.netname, self.netname);
      }
      self.blockReader.rewind(bindex.fileIndex, bindex.pos);
      self.blockReader.readBlocks(n, function(err, blocks) {
	if(err) return c(err);
	blocks.forEach(function(b) {
	  setTimeout(function() {
	    self.handleBlock({message:{block: b.block}});
	  }, 0);
	});
	c();
      });
    }
  ], function(err){
    callback(err);
  });
};

Node.prototype.requireBlocksFromFile = function() {
  var self = this;
  this.store.getTipBlock(function(err, tipBlock) {
    if(err) throw err;
    if(tipBlock) {
      console.info(self.netname, 'getting blocks starting from', tipBlock.hash.toString('hex'), 'to');
      self.getIndexedBlocks({prev_hash:tipBlock.hash}, 20, function(err) {
	if(err) throw err;
	if(tipBlock.prev_hash && Math.random() < 0.3) {
	  console.info(self.netname, 'getting blocks starting from prev ', tipBlock.prev_hash.toString('hex'), 'to');
	  self.getIndexedBlocks({prev_hash:tipBlock.prev_hash}, 1, function(err) {
	    if(err) throw err;
	  });
	}
      });
    } else {
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      console.info(self.netname, 'getting genesis block from', genesisBlock.hash.toString('hex'));
      self.getIndexedBlocks({hash: helper.reverseBuffer(genesisBlock.hash)}, 20, function(err) {
	if(err) throw err;
      });
    }
  });  
};

Node.prototype.requireBlocksFromNet = function() {
  var self = this;

  var conn = this.randomConn();
  if(!conn) {
    console.warn(this.netname, 'No active connections');
    return;
  }
  this.store.getTipBlock(function(err, tipBlock) {
    if(err) throw err;
    if(tipBlock) {
      var gHash = helper.reverseBuffer(tipBlock.hash);
      conn.sendGetBlocks([gHash], 0);
      console.info(self.netname, 'getting blocks starting from', gHash.toString('hex'), 'from', conn.peer.host);
      if(tipBlock.prev_hash && Math.random() < 0.1) {
	var conn1 = self.randomConn();
	if(!!conn1) {
	  var pHash = helper.reverseBuffer(tipBlock.prev_hash);;
	  conn1.sendGetBlocks([pHash], 0);
	  console.info(self.netname, 'getting blocks starting from prev ', pHash.toString('hex'), 'from', conn1.peer.host);
	}
      }
    } else {
      //var gHash = buffertools.fill(new Buffer(32), 0);
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      console.info(self.netname, 'getting genesis block', genesisBlock.hash.toString('hex'), 'from', conn.peer.host);
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
	  //console.info('skip', skip.message);
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
  var txObj = self.processTx(tx, -1);

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

Node.prototype.sendPendingTX = function(callback) {
  var self = this;
  var col = self.store.dbconn.collection('mempool');
  var ctx = false;
  var conns = self.peerman.getActiveConnections();
  if(conns.length == 0) {
    if(typeof callback == 'function') {
       callback();
    }
    return;
  }
  async.doWhilst(function(c) {
    col.findAndModify({pending: true}, [],
		      {$unset: {pending: ''}},
		      {},
		      function(err, tx) {
			if(err) return c(err);
			ctx = tx;

			if(tx && tx.raw) {
			  conns.forEach(function(conn) {
			    console.info('send pending tx', self.netname,
					 tx.hash.buffer.toString('hex'), 'to', conn.peer.host);
			    conn.sendMessage('tx', helper.toBuffer(tx.raw));
			  });
			}
			c();
		      });
  }, function(){
    return !!ctx;
  }, function(err) {
    if(typeof callback == 'function') {
      callback(err);
    }
  });
};

Node.prototype.processMempool = function(tx, callback) {
  var self = this;
  var col = self.store.dbconn.collection('mempool');
  var invs = [];
  var rawTXes = [];

  // Remove txes spawned a week ago
  var lastID = new mongodb.ObjectID.createFromTime(Math.floor(new Date().getTime()/1000 - 86400));
  col.remove({_id: {$lt: lastID}}, function(err) {
    if(err) throw err;
  });
  async.series([
    function(c) {
      col.find({raw: {$exists: true}}).toArray(function(err, txes) {
	txes.forEach(function(tx){
	  tx.hash = helper.toBuffer(tx.hash);
	  invs.push({
	    type: 1,
	    hash: helper.reverseBuffer(tx.hash)
	  });
	  rawTXes.push(helper.toBuffer(tx.raw));
	});
	c();
      });
    },
    function(c) {
      if(invs.length <= 0)  return c();
      var conns = self.peerman.getActiveConnections();
      invs.forEach(function(inv) {
	console.info('send mempool tx', self.netname, helper.reverseBuffer(inv.hash).toString('hex'), 'to', conns.length, 'nodes');
      });
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

Node.prototype.fetchBlock = function(hash, callback) {
  var self = this;
  var bHash = helper.reverseBuffer(hash);
  this.pendingBlockHashes[bHash.toString('hex')] = callback;

  if(!this.fetchTimer) {
    this.fetchTimer = setInterval(function() {
      var hasBlock = false;
      for(var bHashHex in self.pendingBlockHashes) {
	var bHash = new Buffer(bHashHex, 'hex');
	var conn = self.randomConn();
	if(conn) {
	  console.info('fetching block', bHash.toString('hex'), bHash.length);
	  var inv = {type: 2, hash:bHash};
	  conn.sendGetData([inv]);
	}
	hasBlock = true;
      }
      if(!hasBlock) {
	clearInterval(self.fetchTimer);
	self.fetchTimer = undefined;
      }
    }, 3000);
  }
};

Node.prototype.addNewBlock = function(b, cb) {
  var self = this;
  var txList;
  async.series([
      function(c) {
        self.store.insertBlock(b, function(err, txes) {
          if(err) return c(err);
          txList = txes;
          c();
        });
      },
      function(c) {
        if(!txList) return c();
        txList.forEach(function(tx) {
          self._storeTx(tx, function(err) {
            c(err);
          });
        });
      }
      ],
      function(err){
        return cb(err);
      });
};

// if tx exists, update tx's binfo
// else insert tx
Node.prototype._storeTx(tx, cb) {
  var self = this;
  var oldTx;
  async.series([
      function(c) {
        self.store.getTx(tx.hash, function(err, val) {
          if(err) return c(err);
          oldTx = val;
          c();
        });
      },
      function(c) {
        if(!oldTx) return c();
        var needUpdate = true;
        if(oldTx.binfo) {
          oldTx.binfo.forEach(function(entry) {
            if(entry.bhash === tx.bhash
              && entry.bidx === tx.bidx) {
                needUpdate = false;
              }
          });
          if(needUpdate) {
            oldTx.binfo.push({bhash:tx.bhash,bidx:tx.bidx});
          }
        } else {
          oldTx.binfo = [{bhash:tx.bhash,bidx:tx.bidx}];
        }
        if(!needUpdate) return c();
        self.store.updateTx(tx, function(err, val) {
          c(err);
        });
      },
      function(c) {
        if(oldTx) return c();
        self.store.insertTx(tx, function(err) {
          c(err);
        });
      }
      ],
      function(err){
        return cb(err);
      });
};

module.exports = Node;
