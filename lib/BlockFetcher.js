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

function BlockFetcher(netname) {
  var self = this;
  this.blockChain = new BlockChain(netname);
  this.netname = netname;

  this.pendingBlockHashes = {};

  this.peerman = new PeerManager({
    network: this.netname
  });
  //this.peerman.peerLimit = 200;
  this.peerman.peerDiscovery = true;

  this.store = MongoStore.stores[netname];
}

BlockFetcher.prototype.run = function(callback) {
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
    });

    self.peerman.on('netConnected', function(info) {
      if(typeof callback == 'function') {
	callback();
      }
    });
    self.peerman.start();
  });
};

BlockFetcher.prototype.stop = function(cb) {
  this.blockChain.stop(cb);
};

BlockFetcher.prototype.handleBlock = function(info) {
  var block = info.message.block;
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

BlockFetcher.prototype.onBlock = function(block, opts, cb) {
  var self = this;
  var blockHash = block.calcHash(bitcore.networks[self.netname].blockHashFunc);
  var blockHashHex = blockHash.toString('hex');
  var pendingFunc = this.pendingBlockHashes[blockHashHex];
  if(pendingFunc) {
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

    pendingFunc(blockObj);
    delete this.pendingBlockHashes[blockHashHex];
  }
};

BlockFetcher.prototype.fetchBlock = function(hash, callback) {
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

module.exports = BlockFetcher;
