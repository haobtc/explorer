var bitcore = require('bitcore-multicoin');

var url = require('url');
var http = require('http');
var request = require('request');
var mongodb = require('mongodb');
var underscore = require('underscore');
var async = require('async');
var blockstore = require('./blockstore');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;

var Script = bitcore.Script;
var buffertools = require('buffertools');

var hex = function(hex) {return new Buffer(hex, 'hex');};

function Node(netname) {
  var self = this;
  this.netname = netname;

  this.rpcClient = blockstore[this.netname];

  this.updateMempool = true;
  this.updateBlockChain = true;

  this.startHeight = -1;

  this.syncTimer = null;
  this.blockGateDays = 20;

  this.peerman = new PeerManager({
    network: this.netname
  });

  this.peerman.peerLimit = 250;
  this.peerman.peerDiscovery = true;
}

Node.prototype.run = function(callback) {
  var self = this;
  this.rpcClient.getPeers(function(err, v) {
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
      if(self.updateBlockChain) {
	self.startSync();
      }
      //self.rpcClient.keepTip();

      if(typeof callback == 'function') {
	callback();
      }
    });
    self.peerman.start();
  });
};

Node.prototype.stop = function(cb) {
  //this.blockChain.stop(cb);
  process.exit();
};

Node.prototype.savePeers = function(callback) {
  var peers = this.peerman.getActiveConnections().slice(0, 30).map(function(conn) {
    return conn.peer.host + ':' + conn.peer.port;
  });
  this.rpcClient.setPeers(peers, callback||function(err){if(err) throw err});
};

Node.prototype.startSync = function() {
  var self = this;
  if(this.syncTimer) {
    return;
  }

  this.syncTimer = setInterval(function() {
    self.requireBlocks();
  }, 4000 + 2000 * Math.random());

  this.sendTxTimer = setInterval(function() {
    self.sendRawTx(function(err) {
      if(err) throw err;
    });
  }, 3000);

  setTimeout(function() {
    self.requireBlocks();
  }, 1000);
};

Node.prototype.handleBlock = function(info) {
  var block = info.message.block;
  
  this.onBlock(block, {rawMode: false}, function(err) {
    if(err) {
      console.error('error on block', err, err.stack);
    }
  });
};

Node.prototype.handleRawBlock = function(info) {
  //console.info('handleRawBlock');
  var block = new bitcore.Block();
  block.parse(info.message.parser, true);
  this.onBlock(block, {rawMode: true, parser: info.message.parser}, function(err) {
    //if(err) throw err;
    if(err) {
      console.error('error on raw block', err, err.stack);
    }
  });
};

Node.prototype.handleTx = function(info) {
  var self = this;
  //console.info('handle tx', info.message.tx);
  if(this.updateMempool) {
    var tTx = new blockstore.ttypes.Tx();
    tTx.netname(this.netname);
    tTx.fromTxObj(info.message.tx);

    self.rpcClient.verifyTx(tTx, true, function(err, r) {
      if(!r.verified) {
	console.warn('tx not verified ', tTx.hash.toString('hex'), r.message);
      } else {
	self.rpcClient.addTxList([tTx], true, function(err, r) {
	  if(err) throw err;
	});
      }
    });
  }
};

Node.prototype.handleInv = function(info) {
  var invs = info.message.invs;
  var now = new Date();
  var self = this;
  //console.info('handleInv', self.updateMempool, invs.length);

  if(self.endDate && now.getTime() > self.endDate + 5000) {
    console.info('stop from inv');
    self.stop();
    process.exit();
    return;
  }
  /*invs = underscore.filter(invs, function(inv) {
    if(inv.type != 1) return true;
    if(!self.updateMempool) return false;
    return true;
  }); */
  if(invs.length > 0) {
    var tInvs = invs.map(function(inv) {
      var tInv = new blockstore.ttypes.Inventory();
      tInv.type = inv.type;
      tInv.hash = helper.reverseBuffer(inv.hash);
      return tInv;
    });

    self.rpcClient.getMissingInvList(tInvs, function(err, missingInvs) {
      if(err) throw err;
      if(missingInvs.length > 0) {
	var invs = missingInvs.map(function(tInv) {
	  return {
	    'type': tInv.type,
	    'hash': helper.reverseBuffer(tInv.hash)
	  };
	});
	info.conn.sendGetData(invs);
      }
    });
  }
};

Node.prototype.handleGetData = function(info) {
  //console.info('handleGetData');
  return;
  var txIdList = [];
  info.message.invs.forEach(function(inv) {
    if(inv.type == 1) {
      txIdList.push(helper.reverseBuffer(inv.hash));
    }
  });
  if(txIdList.length > 0) {
    this.rpcClient.getSendTxList(txIdList, function(err, arr) {
      if(err) throw err;
      arr.forEach(function(sendtx) {
	if(sendtx.raw) {
	  info.conn.sendMessage('tx', sendtx.raw);
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

  this.rpcClient.getTipBlock(function(err, tipBlock) {
    if(err) {
      if(err instanceof blockstore.ttypes.NotFound) {
	tipBlock = undefined;
      } else {
	throw err;
      }
    }
    if(tipBlock) {
      self.rpcClient.tipBlock = tipBlock;

      var gHash = helper.reverseBuffer(tipBlock.hash);
      conn.sendGetBlocks([gHash], 0);
      console.info(self.netname, 'getting blocks starting from', tipBlock.hash.toString('hex'), 'from', conn.peer.host);

      if(tipBlock.prevHash && Math.random() < 0.2) {
	var conn1 = self.randomConn();
	if(!!conn1) {
	  var pHash = helper.reverseBuffer(tipBlock.prevHash);;
	  conn1.sendGetBlocks([pHash], 0);
	  console.info(self.netname, 'getting blocks starting from prev ', tipBlock.prevHash.toString('hex'), 'from', conn1.peer.host);
	}
      } else if(Math.random() < 0.2){
	self.rpcClient.getTailBlockList(6, function(err, arr) {
	  if(err) throw err;
	  var tBlock = arr[0];
	  if(tBlock) {
	    var conn2 = self.randomConn();
	    if(!!conn2) {
	      var pHash = helper.reverseBuffer(tBlock.hash);;
	      conn2.sendGetBlocks([pHash], 0);
	      console.info(self.netname, 'getting blocks starting from prev5 ', tBlock.hash.toString('hex'), 'from', conn2.peer.host);
	    }
	  }
	});
      }
    } else {
      //var gHash = buffertools.fill(new Buffer(32), 0);
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      console.info(self.netname, 'getting genesis block', helper.reverseBuffer(genesisBlock.hash).toString('hex'), 'from', conn.peer.host);
      var inv = {type: 2, hash:genesisBlock.hash};
      conn.sendGetData([inv]);
    }
  });  
};

Node.prototype.onBlock = function(block, opts, callback) {
  var self = this;
  var txList;
  var txIdList = [];
  var newTxIdList;
  var blockVerified;
  var tBlock = new blockstore.ttypes.Block();
  tBlock.netname(this.netname);
  tBlock.fromBlockObj(block);
  if(!this.updateBlockChain) return callback();
  
  var tipBlock = self.rpcClient.tipBlock;
 
  if(!this.blockGateDays >= 0 &&
     tipBlock &&
     block.timestamp < tipBlock.timestamp - 86400 * this.blockGateDays) {
    //console.info('deny old block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return callback();
  }
  if(!this.blockGateDays >= 0 &&
     tipBlock &&
     block.timestamp > tipBlock.timestamp + 86400 * this.blockGateDays) {
    //console.info('deny new block', this.netname, 'at', new Date(block.timestamp * 1000).toString());
    return callback();
  }

  async.series(
    [
      function(c) {
	if(!self.rpcClient.tipBlock) {
	  var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
	  if(tBlock.hash.toString('hex') != helper.reverseBuffer(genesisBlock.hash).toString('hex')) {
	    return c(new Error('Invalid genesis block ' + tBlock.hash.toString('hex')));
	  }
	}
	c();
      },
      function(c) {
	self.rpcClient.verifyBlock(tBlock, function(err, v) {
	  blockVerified = v.verified;
	  if(!v.verified) {
	    //console.warn('block not verified ' + tBlock.hash.toString('hex') + ' message: ' + v.message);
	    return c();
	  }
	  if(opts.rawMode) block.parseTxes(opts.parser);
	  tBlock.cntTxes = block.txs.length;
	  txList = block.txs.map(function(tx) {
	    var tTx = new blockstore.ttypes.Tx();
	    tTx.netname(self.netname);
	    tTx.fromTxObj(tx);
	    txIdList.push(tTx.hash);
	    return tTx;
	  });
	  c();
	});
      },
      function(c) {
	if(!blockVerified) return c();
	if(txIdList.length ==0) return c();
	self.rpcClient.getMissingTxIdList(txIdList, function(err, arr) {
	  if(err) return c(err);
	  newTxIdList = arr;
	  c();
	});
      },
      function(c) {
	if(!blockVerified) return c();
	if(!newTxIdList || newTxIdList.length == 0) return c();
	var newTxIdMap = {};
	newTxIdList.forEach(function(txId) {
	  newTxIdMap[txId.toString('hex')] = true;
	});

	var newTxList = txList.filter(function(tTx) {
	  return !!newTxIdMap[tTx.hash.toString('hex')];
	});
	if(newTxList.length == 0) return c();
	self.rpcClient.addTxList(newTxList, false, c);
      },
      function(c) {
	if(!blockVerified) return c();
	self.rpcClient.addBlock(tBlock, txIdList, c);
      }
    ],
    callback);
};

Node.prototype.sendRawTx = function(callback) {
  var self = this;
  var sendTxList;
  var removedTxHashList = [];  

  function deliverSendTxes() {
    var conns = self.peerman.getActiveConnections();
    sendTxList.forEach(function(sendtx) {
      console.warn('broadcast tx', self.netname, sendtx.hash, 'to', conns.length, 'peers');
      conns.forEach(function(conn) {
	conn.sendMessage('tx', sendtx.raw);
      });
    });
  }
  async.series([
    function(c) {
      self.rpcClient.getSendingTxList(function(err, arr) {
	if(err) return c(err);
	sendTxList = arr;
	c();
      });
    },
    function(c) {
      if(!sendTxList || sendTxList.length == 0) return c();
      deliverSendTxes();
      c();
    }
  ], callback);
};

module.exports = Node;
