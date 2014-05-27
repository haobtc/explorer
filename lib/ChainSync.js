var bitcore = require('../alliances/bitcore/bitcore');
var MongoStore = require('./MongoStore');
var util = bitcore.util;
var helper = require('./helper');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');

var hex = function(hex) {return new Buffer(hex, 'hex');};

function ChainSync(netname, opts) {
  opts = opts || {};
  this.start_height = -1;
  this.netname = netname;  
  this.peerman = new PeerManager({
    network: this.netname
  });
}

ChainSync.prototype.run = function() {
  var self = this;
  //peerman.addPeer(new Peer('10.132.63.139', 22556));
  //peerman.addPeer(new Peer('115.29.186.22', 22556));
  this.peerman.addPeer(new Peer('192.155.84.55', 22556));
  this.peerman.on('connection', function(conn) {
    conn.on('inv', self.handleInv.bind(self));
    conn.on('block', self.handleBlock.bind(self));
    conn.on('tx', self.handleTx.bind(self));
    conn.on('version', self.handleVersion.bind(self));
    conn.on('verack', self.handleVerAck.bind(self));
  });

  function syncJob() {
    setInterval(function() {
      var store = MongoStore.stores[self.netname];
      if(!store.isPushingBlock()) {
	self.getBlocks();
      } else {
	console.info('pushing block');
      }
    }, 15000);
    setTimeout(function() {
      self.getBlocks();
    }, 1000);
  }

  this.peerman.start();
  syncJob();
}

ChainSync.prototype.handleBlock = function(info) {
  var self = this;
  var block = info.message.block;
  var obj = {
    hash: helper.reverseBuffer(block.calcHash()),
    merkle_root: block.merkle_root,
    height: 0,
    nonce: block.nonce,
    version: block.version,
    prev_hash: helper.reverseBuffer(block.prev_hash),
    timestamp: block.timestamp,
    bits: block.bits
  }

  obj.txes = block.txs.map(function(tx) {
    var txObj = {};
    txObj.hash = helper.reverseBuffer(tx.hash);
    txObj.bhash = obj.hash;
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
      }
      txIn.s = input.s;
      return txIn;
    });

    txObj.vout = tx.outs.map(function(out, i) {
      var txOut = {};
      txOut.s = out.s;
      var script = new Script(tx.outs[i].s);
      txOut.addrs = script.getAddrStr(self.netname);
      txOut.v = util.valueToBigInt(out.v).toString();
      return txOut;
    });
    return txObj;
  });

  var store = MongoStore.stores[this.netname];
  store.pipedPushBlock(obj, function(err) {
    if(err) {
      console.error(err);
    }
  });
};

ChainSync.prototype.handleTx = function(info) {
  var tx = info.message.tx.getStandardizedObject();
//  console.log('** TX Received **', tx);
};

ChainSync.prototype.handleInv = function(info) {
/*    console.log('** Inv **');
    console.log(info.message);
*/
    var invs = info.message.invs;
    info.conn.sendGetData(invs);
};

ChainSync.prototype.handleVersion = function(info) {
  console.log('** Version **', info.message);
  if(info.message.start_height > this.start_height) {
    this.start_height = info.message.start_height;
  }
};

ChainSync.prototype.handleVerAck = function(info) {
    console.log('** Verack **', info.message);
};

ChainSync.prototype.getBlocks = function() {
  var self = this;
  var activeConnections = [];
  this.peerman.connections.forEach(function(conn) {
    if(conn.active) {
      activeConnections.push(conn);
    }
  });
  if(activeConnections.length == 0) {
    console.warn('No active connections');
    return;
  }
  var conn = activeConnections[Math.floor(Math.random() * activeConnections.length)];
  if(!conn) {
    return;
  }
  var store = MongoStore.stores[this.netname];
  store.getLatestBlock(function(err, latestBlock) {
    if(latestBlock) {
      if(latestBlock.height >= self.start_height) {
	return;
      }
      var gHash = helper.reverseBuffer(latestBlock.hash);
    } else {
      var gHash = buffertools.fill(new Buffer(32), 0);
    }
    console.info('getting blocks starting from', gHash.toString('hex'), 'to', conn.peer.host);
    conn.sendGetBlocks([gHash], 0);
  });  
};

module.exports = ChainSync
