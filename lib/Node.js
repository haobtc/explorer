var bitcore = require('bitcore-multicoin');
var http = require('http');
var request = require('request');
var mongodb = require('mongodb');
var underscore = require('underscore');
var async = require('async');
var MongoStore = require('./MongoStore');
var BlockChain = require('./BlockChain');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;

var Script = bitcore.Script;
var buffertools = require('buffertools');
var Skip = helper.Skip;

var hex = function(hex) {return new Buffer(hex, 'hex');};


function RemoteBlockChain(netname) {
  this.netname = netname;
  this.blockQueue = {_size: 0, 
		     size: function() {return this._size},
		     isFull: function() {return this._size >= 1000;}};
}

RemoteBlockChain.prototype.sync = function() {
  var self = this;
  request({
    uri: config.networks[this.netname].remoteBlockChain.url + '/stats/' + this.netname,
    json: true},
	  function(err, response, body) {
	    if(err) {
	      console.error('request error', err);
	    } else {
	      console.info(body, body.queueSize);
	      self.blockQueue._size = body.queueSize;
	    }
	  });
};

RemoteBlockChain.prototype.__enqueueBlock = function (blockContext) {
  var self = this;
  //var rawBlockHex = block.serialize().toString('hex');
  var rawBlockHex = blockContext.raw.toString('hex');
  
  request({
    method: 'POST',
    uri: config.networks[this.netname].remoteBlockChain.url + '/blocks/' + this.netname,
    form: {raw: rawBlockHex }},
	  function(err, response, body) {
	    //console.info('enqueue block', err, body);
	  });
};


RemoteBlockChain.prototype.enqueueBlock = function (blockContext) {
  var self = this;
  var options = {
    port: 19000,
    hostname: 'localhost',
    method: 'POST',
    headers: {'Content-Type': 'application/octet-stream'},
    path: '/blocks/' + this.netname
  };
  var req = http.request(options, function(res){
    var body = '';
    res.setEncoding('utf8');
    res.on('data', function(chunk){
      body += chunk;
    });
    res.on('end', function(){
      console.info('body', body);
    });
  });
  req.write(blockContext.raw.toString('binary'));
  req.end();
};

RemoteBlockChain.prototype.stop = function(cb) {
  cb();
};

function Node(netname) {
  var self = this;
  this.netname = netname;


  this.updateMempool = true;
  this.synchronize = true;

  this.blockCache = {};

  this.startHeight = -1;

  this.syncTimer = null;
  this.allowOldBlock = true;

  this.peerman = new PeerManager({
    network: this.netname
  });
  this.peerman.peerLimit = 100;
  this.peerman.peerDiscovery = true;
  this.store = MongoStore.stores[netname];

  if(this.useRemoteChain()) {
    this.blockChain = new RemoteBlockChain(netname);
  } else {
    this.blockChain = new BlockChain(netname);
  }
}

Node.prototype.useRemoteChain = function() {
  return config.networks[this.netname].remoteBlockChain.enabled;
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
      conn.rawBlockMode = true;
      conn.on('inv', self.handleInv.bind(self));
      conn.on('block', self.handleBlock.bind(self));
      conn.on('rawblock', self.handleRawBlock.bind(self));
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

Node.prototype.stop = function(cb) {
  this.blockChain.stop(cb);
};

Node.prototype.savePeers = function(callback) {
  var peers = this.peerman.getActiveConnections().slice(0, 30).map(function(conn) {
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
    console.info('queue length', self.netname, self.blockChain.blockQueue.size());
    if(self.blockChain.blockQueue.size() < 5) {
      self.requireBlocks();
    } else {
      console.info('queue length', self.netname, self.blockChain.blockQueue.size());
    }
  }, 4000 + 2000 * Math.random());

  this.sendTxTimer = setInterval(function() {
    self.sendRawTx(function(err) {
      if(err) throw err;
    });
  }, 3000);

  this.cleanupBlockTimer = setInterval(function() {
    self.cleanupBlocks(function(err) {
      if(err) throw err;
    });
  }, 1000 * 180);

  this.cleanupTxesTimer = setInterval(function() {
    self.cleanupTxes(function(err) {
      if(err) throw err;
    });
  }, 1000 * 190);

  setInterval(function() {
    self.blockChain.sync();
  }, 3000);

  setTimeout(function() {
    self.requireBlocks();
  }, 1000);
};

Node.prototype.handleBlock = function(info) {
  var block = info.message.block;
  this.onBlock(block, {rawMode: false}, function(err, q) {
    if(err) throw err;
  });
};

Node.prototype.handleRawBlock = function(info) {
  var block = new bitcore.Block();
  block.parse(info.message.parser, true);
  this.onBlock(block, {rawMode: true, parser: info.message.parser}, function(err, q) {
    if(err) throw err;
  });
};

Node.prototype.handleTx = function(info) {
  var self = this;
  if(this.updateMempool) {
    var txObj = helper.processTx(this.netname, info.message.tx, -1);
    self.store.verifyTx(txObj, function(err, verified) {
      if(err) throw err;
      if(verified) {
	self.store.addTxes([txObj], function(err) {
	  if(err) throw err;
	});
      } else {
	console.info('tx', txObj.hash.toString('hex'), 'is not verified');
      }
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
    this.store.getTxes(txHashList, function(err, txes) {
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
  if(info.message.start_height > this.startHeight) {
    this.startHeight = info.message.start_height;
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

Node.prototype.onBlock = function(block, opts, cb) {
  var self = this;
  var blockHash = block.calcHash(bitcore.networks[self.netname].blockHashFunc);
  var blockHashHex = blockHash.toString('hex');

  if(!this.allowOldBlock &&
     this.store.stats.tipTimestamp > 0 &&
     block.timestamp < this.store.stats.tipTimestamp - 86400 * 2) {
    //console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return cb(null, false);
  }
  if(!this.allowOldBlock &&
     this.store.stats.tipTimestamp > 0 &&
     block.timestamp > this.store.stats.tipTimestamp + 86400 * 2) {
    //console.info('deny new block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return cb(null, false);
  }

  if(this.blockChain.blockQueue.isFull()) {
    console.info('block is full');
    return cb(null, false);
  }

  var hit = false;
  if(this.blockCache.hasOwnProperty(blockHashHex)) {
    //console.info('hit', blockHashHex);
    hit = true;
    var blockContext = this.blockCache[blockHashHex];
  } else {
    if(this.useRemoteChain()) {
      var blockObj = null;
    } else {
      var blockObj = {
	hash: helper.reverseBuffer(blockHash),
	merkle_root: block.merkle_root,
	height: 0,
	nonce: block.nonce,
	version: block.version,
	prev_hash: helper.reverseBuffer(block.prev_hash),
	timestamp: block.timestamp,
	bits: block.bits
      };
      if(opts.rawMode) {
	block.parseTxes(opts.parser);
      }
      blockObj.txes = block.txs.map(function(tx, idx) {
	return helper.processTx(self.netname, tx, idx, blockObj);
      });
    }

    var blockContext = {
      blockObj: blockObj,
      block: block,
      raw: opts.parser.subject
    };
    this.blockCache[blockHashHex] = blockContext;
  }

  var queued = self.blockChain.enqueueBlock(blockContext);
  return cb(null, queued);
};

Node.prototype.sendRawTx = function(callback) {
  var self = this;
  var col = this.store.dbconn.collection('sendtx');
  var txCol = this.store.dbconn.collection('tx');
  var sendTxList;
  
  function deliverSendTxes() {
    var conns = self.peerman.getActiveConnections();
    sendTxList.forEach(function(sendtx) {
      conns.forEach(function(conn) {
	console.info('send pending tx', self.netname, sendtx.hash.toString('hex'), 'to', conn.peer.host);
	conn.sendMessage('tx', helper.toBuffer(sendtx.raw));
      });
    });
  }
  async.series([
    function(c) {
      col.find({sent: false}).limit(20).toArray(function(err, arr) {
	if(err) return c(err);
	sendTxList = arr;
	sendTxList.forEach(function(sendtx) {
	  sendtx.hash = helper.toBuffer(sendtx.hash);
	});
	c();
      });
    },
    function(c) {
      var txHashList = sendTxList.map(function(tx) {return tx.hash;});
      if(txHashList.length == 0) return c();
      self.store.getTxes(txHashList, function(err, txes) {
	if(err) return c(err);
	var existingTxHashList = [];
	txes.forEach(function(tx) {
	  if(tx.bhs && tx.bhs.length > 0) {
	    // exist
	    existingTxHashList.push(tx.hash);
	  }
	});
	sendTxList = underscore.reject(sendTxList, function(sendtx) {
	  return underscore.contains(existingTxHashList, sendtx.hash);
	});
	console.info('mark txes to be sent', existingTxHashList);
	col.update({hash: {$in: existingTxHashList}}, {sent: true}, {multi: true}, c);
      });      
    },
    function(c) {
      if(!sendTxList || sendTxList.length == 0) return c();
      deliverSendTxes();
      c();
    }
  ], callback);
};

Node.prototype.cleanupBlocks = function(callback) {
  var self = this;
  var col = this.store.dbconn.collection('block');
  var currDate = new Date();
  var startObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - 2 * 86400);
  var endObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - 86400);
  col.find({_id: {$gt:startObjectId, $lt:endObjectId}}).toArray(function(err, arr) {
    arr = arr || [];
    console.info('CLEANUP TASK: found blocks', arr.length);
    var fns = [];
    arr.forEach(function(block) {
      if(!block.isMain) {
	fns.push(function(c) {
	  self.store.removeBlock(c);
	});
      }
    });
    if(fns.length > 0) {
      async.series(fns, callback);
    } else {
    }
  });
};

Node.prototype.cleanupTxes = function(callback) {
  var self = this;
  var col = this.store.dbconn.collection('tx');
  var currDate = new Date();
  var startObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - 2 * 86400);
  var endObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - 86400);
  var removableTxes = [];
  col.find({_id: {$gt:startObjectId, $lt:endObjectId}}).toArray(function(err, arr) {
    arr = arr || [];
    console.info('CLEANUP TASK: found txes', arr.length);
    var fns = [];
    arr.forEach(function(tx) {
      if(!tx.bhs || tx.bhs.length == 0) {
	removableTxes.push(tx);
      }
    });
    if(removableTxes.length > 0) {
      self.store.removetxes(removableTxes, callback);
    } else {
      callback();
    }    
  });
}

module.exports = Node;
