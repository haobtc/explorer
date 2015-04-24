var bitcore = require('bitcore-multicoin');
var async = require('async');
var MongoStore = require('./MongoStore');
var BlockChain = require('./BlockChain');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var blockstore = require('./blockstore');

var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;

var Script = bitcore.Script;
var buffertools = require('buffertools');
var Skip = helper.Skip;

var hex = function(hex) {return new Buffer(hex, 'hex');};

function BlockFetcher(netname, blockHashes) {
  this.netname = netname;
  this.blockHashes = blockHashes;

  this.peerman = new PeerManager({
    network: this.netname
  });
  this.peerman.peerDiscovery = true;
  this.rpcClient = blockstore[this.netname];
  this.pendingBlocks = [];
}

BlockFetcher.prototype.checkBlockHashes = function(callback) {
  var self = this;

  fns = this.blockHashes.map(function(bHash) {
    return function(c) {
      self.rpcClient.getBlock(bhash, function(err, b) {
	if(err) {
	  if(err instanceof blockstore.ttypes.NotFound) {
	    b = null;
	  } else{
	    return c(err);
	  }
	}
	if(b && b.isMain) {
	  self.pendingBlocks = b;
	}
	c();
      });
    };
  });
  async.series(fns, function(err) {
    if(err) return callback(err);
    if(self.pendingBlocks.length == 0) {
      return c(new Error('No valid block hash given'));
    }
    return c();
  });
};

BlockFetcher.prototype.run = function(callback) {
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
      conn.on('inv', self.handleInv.bind(self));
      conn.on('block', self.handleBlock.bind(self));
    });

    self.peerman.on('netConnected', function(info) {
      if(typeof callback == 'function') {
	callback();
      }
    });
    self.peerman.start();
    callback();
  });
};

BlockFetcher.prototype.stop = function(cb) {
  this.blockChain.stop(cb);
};

BlockFetcher.prototype.handleBlock = function(info) {
  var block = info.message.block;
  var bHash = blockObj.calcHash(bitcore.networks[self.netname()].blockHashFunc);
  
  

  this.onBlock(block, {rawMode: false}, function(err, q) {
    if(err) throw err;
  });
};

BlockFetcher.prototype.handleInv = function(info) {
  var invs = info.message.invs;
  info.conn.sendGetData(invs);
};

BlockFetcher.prototype.randomConn = function() {
  var activeConnections = this.peerman.getActiveConnections();
  if(activeConnections.length == 0) {
    console.warn(this.netname, 'No active connections');
    return;
  }
  var conn = activeConnections[Math.floor(Math.random() * activeConnections.length)];
  return conn;
};


BlockFetcher.prototype.onBlock = function(block, opts, callback) {
  var self = this;
  var txIdList = [];
  var newTxIdList;
  var blockVerified;
  var tBlock = new blockstore.ttypes.Block();
  tBlock.netname(this.netname);
  tBlock.fromBlockObj(block);

  if(opts.rawMode) block.parseTxes(opts.parser);
  tBlock.cntTxes = block.txs.length;
  var txList = block.txs.map(function(tx) {
    var tTx = new blockstore.ttypes.Tx();
    tTx.netname(self.netname);
    tTx.fromTxObj(tx);
    txIdList.push(tTx.hash);
    return tTx;
  });
  
  async.series(
    [
      function(c) {
	if(txIdList.length ==0) return c();
	self.rpcClient.getMissingTxIdList(txIdList, function(err, arr) {
	  if(err) return c(err);
	  newTxIdList = arr;
	  c();
	});
      },
      function(c) {
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
	self.rpcClient.linkBlock(tBlock.hash, txIdList, c);
      }
    ],
    callback);
};

BlockFetcher.prototype.start = function(hash, callback) {
  var self = this;


  async.series([
    function(c) {
      return self.checkBlockHashes(c);
    },
    function(c) {
      return self.run(c);
    },
  ]);

  this.run(function(){
    self.fetchBlock(blockHash, function(blockObj) {
      console.info('got block w/', blockObj.txes.length, 'txes');
      
      self.blockChain.updateBlock(blockObj, function(err) {
	if(err) {
	  console.info(err);
	  throw err;
	}
	console.info('updated');
	process.exit();
      });
      });
  });
};

module.exports = BlockFetcher;
