var bitcore = require('../alliances/bitcore/bitcore');
var getenv = require('getenv');
var async = require('async');
var mongodb = require('mongodb');
var events = require('events');
var MongoStore = require('./MongoStore2');
var BlockReader = require('./BlockReader');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');
var Queue = require('./queue');
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
  this.collectBlock = true;
  this.tip_timestamp = 0;

  this.processSpentQueue = new Queue({timeout:1000000, limit:1810});
  this.processingSpent = false;
  this.processingSpentCursor = null;
  this.removingObsolete = false;

  this.pendingBlockHashes = {};

  this.peerman = new PeerManager({
    network: this.netname
  });
  this.peerman.peerDiscovery = true;

  this.store = MongoStore.stores[netname];
  this.events = new events.EventEmitter();
}

Node.prototype.on = function(event, listener) {
  this.events.on(event, listener);
};

Node.prototype.emit = function() {
  this.events.emit.apply(this.events, arguments);
};

/*Node.prototype.updateBlock = function(block, callback) {
  var self = this;
  var txes = block.txes;

  delete block.height;
  delete block._id;
  delete block.txes;

  async.series([
    function(c) {
      self.store.updateBlock(block, function(err, b) {
        if(err) return c(err);
        if(!b) return c(new Skip('block not found'));
        c();
      });
    },
    function(c) {
      self.store.removeTxInBlock(block.hash, c);
    },
    function(c) {
      var txarr = [];
      txarr = txes.map(function(tx) { return tx.hash});
      self.store.getTxList(txarr, function(err, arr) {
        if(err) return c(err);
	arr.forEach(function(t) {
	  console.info('yyy', t.hash.toString('hex'), t.bhash.toString('hex'));
        }
        c();
      });
    },
    function(c) {
      txes.forEach(function(tx) {
        self.store._storeTx(tx, false, function(err) {
          c(err);
        });
      });
    }
  ], function(err) {
    if(err instanceof Skip) {
      err = undefined;
    }
    callback(err);
  });
};*/

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
  var txCnt = b.txes.length;
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
      self.store.getBlockCount(function(err, n) {
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
	    return c(new Skip('Ignore block due to non existing prev: ' + b.hash.toString('hex')));
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
       var skip = (err && (err instanceof Skip)) ? true : false;
       console.info('storetipblock cost too long:hash=%s:skip=%d:height=%d:txCnt=%d:cost=%d',
                    b.hash.toString('hex'), skip, b.height, txCnt, sumtime, tss);
     }
     if(!err) {
       console.info('storetipblock success:hash=%s:height=%d:txCnt=%d:cost=%d',
                    b.hash.toString('hex'), b.height, txCnt, sumtime);
     }
     if(err && err instanceof Skip) {
       //console.info('SKIP', err.message);
       self.events.emit('block skip', b.hash.toString('hex'));
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

Node.prototype.archiveTxes = function(blockHashList, cb) {
  console.info('archiveTxes', blockHashList);
};

Node.prototype.unArchiveTxes = function(blockHashList, cb) {
  console.info('unArchiveTxes', blockHashList);
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
    self.requireBlocks();
    /*console.info('queue length', self.netname, self.blockQueue.size());
    if(self.blockQueue.size() < 5) {
      self.requireBlocks();
    } else {
      console.info('queue length', self.netname, self.blockQueue.size());
    }*/
  }, 10000 + 2000 * Math.random());

  this.mempoolTimer = setInterval(function() {
    self.processMempool(function(err) {
      if(err) console.error(err);
    });
  }, 30000);

  this.pendingTimer = setInterval(function() {
    self.sendPendingTX(function(err) {
      if(err) throw err;
    });
  }, 3000);

  /*
  this.processSpentTimer = setInterval(function() {
    if(self.processingSpent) return;
    self.processingSpent= true;
    self.processSpentQueue.task({timestamp:new Date(), cursor:self.processingSpentCursor},
                                function(c) {
                                  self._processSpent(function(err, finish) {
                                    if(err) console.error(err);
                                    self.processingSpent = false;
                                    c();
                                  });
                                });
  }, 1000);*/

  /*this.removeObsoleteTimer = setInterval(function() {
    self.processSpentQueue.task({timestamp:new Date(), cursor:'remove obsolete'},
                                function(c) {
                                  self.handleObsoleteTxList(function(err) {
                                    if(err) console.error(err);
                                    c();
                                  });
                                });

  }, 30*1000);*/

  setTimeout(function() {
    self.requireBlocks();
    //self.processMempool();
  }, 1000);
};

Node.prototype.processTx = function(tx, idx, blockObj) {
  return helper.processTx(this.netname, tx, idx, blockObj);
};

Node.prototype.handleBlock = function(info) {
  if(!this.collectBlock) return;
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
       this.tip_timestamp > 0 &&
       block.timestamp < this.tip_timestamp - 86400 * 10) {
      console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
      return cb(null, false);
    }
    if(!this.allowOldBlock &&
       this.tip_timestamp > 0 &&
       block.timestamp > this.tip_timestamp + 86400 * 20) {
      console.info('deny new block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
      return cb(null, false);
    }

    /*if(this.blockQueue.isFull()) {
      console.info('block is full');
      return cb(null, false);
    }*/
  }
  var bhash = helper.reverseBuffer(block.calcHash(bitcore.networks[self.netname].blockHashFunc));

  var blockObj = {
    hash: bhash,
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

  console.log('** Block Received **', self.netname, blockObj.hash.toString('hex'), 'at', new Date(blockObj.timestamp * 1000).toString());

  if(pendingFunc) {
    pendingFunc(blockObj);
    delete this.pendingBlockHashes[block.calcHash(bitcore.networks[self.netname].blockHashFunc).toString('hex')];
  }
  this.events.emit('block', blockObj);
  cb(null, true);
};

Node.prototype.stop = function(cb) {
  var self = this;
  console.info('stop signal received');
  this.blockQueue.task({timestamp: 0, hexHash: ''},
                       function(c) {
                         self.blockQueue.closed = true;
                         console.info('block queue closed');
                         cb();
                         c();
                       }, {use:'close block queue'});
  /*async.series([
    function(cc) {
      self.blockQueue.task({timestamp: 0, hexHash: ''},
        function(c){
          self.blockQueue.closed = true;
          cc();
          console.info('block queue closed');
          c();
        }, {use:'close block queue'});
    },
    function(cc) {
      self.processSpentQueue.task({timestamp:new Date(), cursor:''},
        function(c) {
          self.processSpentQueue.closed = true;
          cc();
          console.info('process spent queue closed');
          c();
        }, {use:'close process spent queue'});
    }
  ],
  function(err) {
    if(err) console.error(err);
    cb();
  });*/
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
  if(this.updateMempool) {
    var txObj = this.processTx(info.message.tx, -1);
    this.store.getTx(txObj.hash, function(err, val) {
      if(err) throw err;
      if(val) return;
      self._addTx(txObj, true, function(err, exist) {
        if(err) throw err;
        if(!exist) {
          console.info('addTx:mempool:txHash=%s', txObj.hash.toString('hex'));
        }
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
    this.store.getMempoolTxList(txHashList, function(err, txes) {
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
      self.tip_timestamp = tipBlock.timestamp;
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

Node.prototype.sendTx = function(tx, cb) {
  if(!this.collectBlock) return;
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

  txObj.raw = tx.serialize();
  self._addTx(txObj, true, function(err, exist) {
    if(err) return cb(err, false);
    if(exist) return cb(undefined, {code:-5, message:'transaction already in block chain.'});
    cb(undefined, txObj.hash.toString('hex'));
    deliverToSiblings();
  });
};

/*Node.prototype.sendTxTest = function(txObj, cb) {
  var self = this;
  self._addTx(txObj, true, function(err, exist) {
    if(err) return cb(err, false);
    if(exist) return cb(undefined, {code:-5, message:'transaction already in block chain.'});
    self._onTxAdd(txObj, function(err) {
      if(err) return cb(err, false);
      cb(undefined, txObj.hash.toString('hex'));
    });
  });
};*/

Node.prototype.sendPendingTX = function(callback) {
  var self = this;
  var ctx = false;
  var conns = self.peerman.getActiveConnections();
  if(conns.length == 0) {
    if(typeof callback == 'function') {
       callback();
    }
    return;
  }
  async.doWhilst(function(c) {
    self.store.getPendingTx(function(err, tx) {
      if(err) return c(err);
      if(tx && tx.raw) {
        conns.forEach(function(conn) {
          console.info('send pending tx:netname=%s:txhash=%s:peerHost=%s',
            self.netname, tx.hash.toString('hex'), conn.peer.host);
          conn.sendMessage('tx', tx.raw);
        });
        c();
      }
    });
  },
  function() {
    return !!ctx;
  },
  function(err) {
    if(typeof callback == 'function') {
      callback(err);
    }
  });
};

Node.prototype.handleObsoleteTxList = function(cb) {
  var self = this;
 // Remove txes spawned a day ago
  this.store.getObsoleteTxList(function(err, blockHashList, txList) {
    if(err) return cb(err);
    console.info('obsolete:', blockHashList, txList);
    var tasks = [];
    txList.forEach(function(tx) {
      if(tx._id > self.processSpentCursor) return;
      tasks.push(function(c) {
         self._onTxDel(tx, function(err) {
           if(err) return c(err);
           c();
         });
      });
    });
    tasks.push(function(c) {
      self.store.removeObsoleteTxList(txList, function(err) {
        if(err) return c(err);
        c();
      });
    });
    tasks.push(function(c) {
      self.store.removeObsoleteBlockList(blockHashList, function(err) {
        if(err) return c(err);
        c();
      });
    });
    async.series(tasks, function(err) {
      if(err) return cb(err);
      cb(undefined);
    });
  });
};

Node.prototype.processMempool = function(cb) {
  var self = this;
  var invs = [];
  var rawTXes = [];

  async.series([
    function(c) {
      self.store.getRawTxList(function(err, txes) {
        txes.forEach(function(tx) {
          tx.hash = helper.toBuffer(tx.hash);
          invs.push({type:1,hash:helper.reverseBuffer(tx.hash)});
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
  ], cb);
};

// Extra tasks on modification of txes
/*Node.prototype.updateSpent = function(txes) {
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
};*/

Node.prototype.addNewBlock = function(b, cb) {
  var self = this;
  this.store.insertBlock(b, function(err, txes) {
    if(err) return cb(err);
    //console.log('addNewBlock:netname=%s:bhash=%s:height=%d:txes length=%d',
    //            self.netname, b.hash.toString('hex'), b.height, txes.length);
    var tasks = txes.map(function(tx) {
      return function(c) {
        self._addTx(tx, false, function(err, exist) {
          if(err) return c(err);
          //console.log('addNewBlock:addTx:hash=%s:exist=%d', tx.hash.toString('hex'), exist);
          c();
        });
      };
    });
    async.series(tasks, function(err) {
      if(err) return cb(err);
      cb(undefined);
    });
  });
};

Node.prototype._addTx = function(tx, mempool, cb) {
  this.store.insertTx(tx, function(err, oldTx) {
    if(err) return cb(err);
    return cb(undefined, oldTx !== null);
  });
};

// tx delete:
Node.prototype._onTxDel = function(tx, cb) {
  var self = this;
  var spentChangeList = [], receiveChangeList = [], otherVoutTasks = [], selfVoutTasks = [];
  tx.vin.forEach(function(input, i) {
    if(!input.hash) return;
    otherVoutTasks.push(function(c) {
      var spentInfo = {};
      spentInfo['vout.'+input.n+'.spent'] = tx.hash.toString('hex');
      self.store.updateSpent(input.hash, {$pull:spentInfo}, function(err, oldInputTx) {
        if(err) return cb(err);
        if(!oldInputTx) {
          console.error('_onTxDel:inputTx not found:tx=%s:input=%s',
                        tx.hash.toString('hex'),input.hash.toString('hex'));
          return c();
        }
        // check spentTxId in oldInputTx
        var voutEntry = oldInputTx.vout[input.n];
        var exist = false;
        voutEntry.spent.forEach(function(txId) {
          if(txId == tx.hash.toString('hex')) exist = true;
        });
        if(!exist) {
          console.error('_onTxDel:spentTxId not exist:tx=%s:input=%s',
                        tx.hash.toString('hex'),input.hash.toString('hex'));
          return c();
        }
        // balance will increase
        if(voutEntry.spent.length === 1) {
          voutEntry.addrs.forEach(function(addr) {
            spentChangeList.push({a:addr, v:Number(voutEntry.v)});
          });
        }
        c();
      });
    });
  });
  tx.vout.forEach(function(output, i) {
    selfVoutTasks.push(function(c) {
      // receive and balance will decrease
      output.addrs.forEach(function(addr) {
        receiveChangeList.push({a:addr, v:-Number(output.v)});
      });
      c();
    });
  });
  async.series([
    function(c) {
      async.parallel(otherVoutTasks, function(err) {c(err);});
    },
    function(c) {
      async.parallel(selfVoutTasks, function(err) {c(err);});
    },
    function(c) {
      self._onAddrPropChanged(spentChangeList, receiveChangeList, function(err) {c(err);});
    }
  ],
  function(err) {
    if(err) return cb(err);
    cb(undefined);
  });
};

Node.prototype._onTxAdd = function(tx, cb) {
  var self = this;
  var spentChangeList = [], receiveChangeList = [], otherVoutTasks = [], selfVoutTasks = [];
  tx.vin.forEach(function(input, i) {
    if(!input.hash) return;
    otherVoutTasks.push(function(c) {
      var spentInfo = {};
      spentInfo['vout.'+input.n+'.spent'] = tx.hash.toString('hex');
      self.store.updateSpent(input.hash, {$addToSet:spentInfo}, function(err, oldInputTx) {
        if(err) return cb(err);
        if(!oldInputTx) return c();
        // balance will decrease
        var voutEntry = oldInputTx.vout[input.n];
        if(voutEntry.spent.length === 0) {
          voutEntry.addrs.forEach(function(addr) {
            spentChangeList.push({a:addr, v:-Number(voutEntry.v)});
          });
        }
        c();
      });
    });
  });
  tx.vout.forEach(function(output, i) {
    selfVoutTasks.push(function(c) {
      // receive and balance will increase
      output.addrs.forEach(function(addr) {
        receiveChangeList.push({a:addr, v:Number(output.v)});
      });
      var k = tx.hash.toString('hex')+'#'+i;
      self.store.getTxListByVout(k, function(err, txes) {
        if(!txes || txes.length === 0) return c();
        if(txes.length > 1) {
          console.warn('_onTxAdd:k=%s:length=%d', k, txes.length);
        }
        var txArr = [];
        txes.forEach(function(outTx) {
          txArr.push(outTx.hash.toString('hex'));
        });
        var spentInfo = {};
        spentInfo['vout.'+i+'.spent'] = {$each:txArr};
        self.store.updateSpent(tx.hash, {$addToSet:spentInfo}, function(err, oldTx) {
          if(err) return c(err);
          var voutEntry = oldTx.vout[i];
          // balance will decrease
          if(voutEntry.spent.length === 0) {
            voutEntry.addrs.forEach(function(addr) {
              spentChangeList.push({a:addr, v:-Number(voutEntry.v)});
            });
          }
          c();
        });
      });
    });
  });
  async.series([
    function(c) {
      async.parallel(otherVoutTasks, function(err) {c(err);});
    },
    function(c) {
      async.parallel(selfVoutTasks, function(err) {c(err);});
    },
    function(c) {
      self._onAddrPropChanged(spentChangeList, receiveChangeList, function(err) {c(err);});
    }
  ],
  function(err) {
    if(err) return cb(err);
    cb(undefined);
  });
};
Node.prototype._onAddrPropChanged = function(spentChangeList, receiveChangeList, cb) {
  var self = this;
  //console.log('_onAddrPropChanged:spentChangeList', spentChangeList);
  //console.log('_onAddrPropChanged:receiveChangeList', receiveChangeList);
  // addr's balance change
  spentChangeList.forEach(function(spentChangeInfo) {
    // spentChangeInfo, 0->positive: balance decrease, positive->0: balance increase
    self.store.updateAddrProp(spentChangeInfo.a, {balance:spentChangeInfo.v}, function(err) {
      if(err) cb(err);
    });
  });
  receiveChangeList.forEach(function(receiveChangeInfo) {
    // receiveChangeInfo: change addr's total received value
    var updateInfo = {totalReceived:receiveChangeInfo.v, balance:receiveChangeInfo.v};
    self.store.updateAddrProp(receiveChangeInfo.a, updateInfo, function(err) {
      if(err) cb(err);
    });
  });
  cb(undefined);
};

Node.prototype._processSpent = function(cb) {
  var self = this;
  this.store.getTipTx(function(err, val) {
    if(err) return cb(err);
    if(!val) {
      console.warn('no tip tx');
      return cb(undefined, false);
    }
    self.processingSpentCursor = val.objID;
    var fetchStart = new Date();
    self.store.getTxListBySinceID(val.objID, 2000, function(err, txes) {
      if(err) return cb(err);
      if(!txes || txes.length === 0) return cb(undefined, true);
      var s = new Date();
      var tasks = txes.map(function(tx) {
        return function(c) {
          var txAddStart = new Date();
          self._onTxAdd(tx, function(err) {
            if(err) return c(err);
            console.log('_onTxAdd:txHash=%s:cost=%d', tx.hash.toString('hex'), (new Date()-txAddStart)/1000);
            c();
          });
        };
      });
      var prepareTime = (new Date()-s)/1000;
      async.series(tasks, function(err) {
        if(err) return cb(err);
        console.log('_processSpent:sinceID=%s:txHash=%s:txes.length=%d:fetchTime=%d:prepareTime=%d:costTime=%d',
                    val.objID, val.txHash.toString('hex'), txes.length, (s-fetchStart)/1000, prepareTime, (new Date()-s)/1000);
        var lastTx = txes[txes.length-1];
        self.store.setTipTx(lastTx._id, lastTx.hash, function(err) {
          if(err) return cb(err);
          self.processingSpentCursor = lastTx._id;
          cb(undefined, false);
        });
      });
    });
  });
};

module.exports = Node;
