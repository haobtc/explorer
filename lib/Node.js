var bitcore = require('bitcore-multicoin');
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

function Node(netname) {
  var self = this;

  this.blockChain = new BlockChain(netname);
  this.updateMempool = true;
  this.synchronize = true;

  this.blockCache = {};
  this.addBlocks = true;

  this.startHeight = -1;
  this.netname = netname;
  this.syncTimer = null;
  this.allowOldBlock = true;

  this.peerman = new PeerManager({
    network: this.netname
  });
  //this.peerman.peerLimit = 200;
  this.peerman.peerDiscovery = true;

  this.store = MongoStore.stores[netname];
}

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
  return;
  var self = this;
  //  var tx = info.message.tx.getStandardizedObject();
  if(this.updateMempool) {
    var txObj = this.processTx(info.message.tx, -1);
    this.blockChain.addTxes([txObj], function(err) {
      if(err) throw err;
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
     this.store.tip_timestamp > 0 &&
     block.timestamp < this.store.tip_timestamp - 86400 * 10) {
    //console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return cb(null, false);
  }
  if(!this.allowOldBlock &&
     this.store.tip_timestamp > 0 &&
     block.timestamp > this.store.tip_timestamp + 86400 * 20) {
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
    var blockObj = this.blockCache[blockHashHex];
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
    this.blockCache[blockHashHex] = blockObj;
  }

  if(this.addBlocks) {
    var queued = self.blockChain.enqueueBlock(blockObj);
    return cb(null, queued);
  } else {
    return cb(null, false);
  }
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

  this.store.getTx(txObj.hash, function(err, obj) {
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

module.exports = Node;
