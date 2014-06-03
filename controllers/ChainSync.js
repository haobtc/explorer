var bitcore = require('../alliances/bitcore/bitcore');
var mongodb = require('mongodb');
var MongoStore = require('../lib/MongoStore');
var util = bitcore.util;
var helper = require('../lib/helper');
var config = require('../lib/config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');

var hex = function(hex) {return new Buffer(hex, 'hex');};


function toBuffer(hash) {
  if(hash instanceof mongodb.Binary) {
    return hash.buffer;
  } else if(hash instanceof Buffer) {
    return hash;
  } else {
    return new Buffer(hash, 'hex');
  }
}

function ChainSync(netname, opts) {
  opts = opts || {};
  this.netname = netname;
  this.start_height = -1;
  this.fifoAddBlock = new helper.CallFIFO();
  this.pendingBlocks = {};
  var store = MongoStore.stores[netname];
  this.conn = store.conn();
  this.peerman = new PeerManager({
    network: this.netname
  });
}

ChainSync.prototype.findPendings = function(block, callback) {
  var col = this.conn.collection('block');
  for(var pHashString in this.pendingBlocks) {
    var pBlock = this.pendingBlocks[pHashString];
    if(pBlock && pHashString == block.hash.toString('hex')) {
      delete this.pendingBlocks[pHashString];
      pBlock.height = block.height + 1;
      return this.saveBlock(pBlock, callback);
    }
  }
  callback();
};


ChainSync.prototype.removeOrphanBlocks = function(block, callback) {
  var self = this;
  var col = this.conn.collection('block');
  col.find({height: {$gt: block.height}}).toArray(function(err, results) {
    if(err) {
      return callback(err);
    }
    results.forEach(function(sb) {
      delete sb._id;
      self.pendingBlocks[sb.hash.buffer.toString('hex')] = block;
    });
    
    if(results.length > 0) {
      col.remove({height: {$gt: block.height}}, callback);
    } else {
      callback();
    }
  });
};

ChainSync.prototype.saveBlock = function(block, callback) {
  var self = this;
  var col = this.conn.collection('block');
  if(typeof callback != 'function') {
    callback = function(err) {if(err){console.error(err)}};
  }
  delete block._id;
  var txes = block.txes;
  delete block.txes;
  console.info('save block', block.hash.toString('hex'));
  col.findAndModify(
    {'hash': block.hash}, [],
    {$set: block},
    {upsert: true},
    function(err) {
      if(err) {
	return callback(err);
      }
      if(txes && txes.length > 0) {
	var txcol = self.conn.collection('tx');
	txcol.insert(txes, function(err) {
	  if(err) {
	    return callback(err);
	  }
	  self.findPendings(block, callback);
	});
      } else {
	self.findPendings(block, callback);
      }
    });
};

ChainSync.prototype.addBlock = function(block, callback) {
  var self = this;
  console.log('** Block Received **', block.hash.toString('hex'), block.prev_hash.toString('hex'));
  var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
  var genesisBlockHash = helper.reverseBuffer(genesisBlock.hash).toString('hex');

  var col = this.conn.collection('block');
  this.getBlock(block.hash, function(err, aBlock) {
    if(err)
      return callback(err);
    if(!!aBlock) {	// Already have the block, return;
      return callback();
    }
    col.findOne(function(err, firstBlock) {
      if(err)
	return callback(err);
      if(firstBlock) {
	// Already have at least one block
	self.getBlock(block.prev_hash, function(err, prevBlock) {
	  if(err)
	    return callback(err);

	  if(prevBlock) {
	    self.removeOrphanBlocks(prevBlock, function(err) {
	      if(err)
		return callback(err);

	      block.height = prevBlock.height + 1;
	      self.saveBlock(block, callback);
	    });
	  } else {
	    self.pendingBlocks[block.hash.toString('hex')] = block;	    
	    return callback();
	  }
	});
      } else if(block.hash.toString('hex') == genesisBlockHash) {
	self.saveBlock(block, callback);
      } else {
	self.pendingBlocks[block.hash.toString('hex')] = block;
	return callback();
      }
    });
  });
};

ChainSync.prototype.getLatestBlock = function(callback) {
  var col = this.conn.collection('block');
  col.find().sort({height: -1}).limit(1).toArray(function(err, results) {
    if(err) {
      return callback(err);
    }
    var block = results[0];
    if(block) {
      block.hash = toBuffer(block.hash);
      block.prev_hash = toBuffer(block.prev_hash);
    }
    callback(err, block);
  });
};


ChainSync.prototype.getBlock = function(hash, callback) {
  var col = this.conn.collection('block');
  col.find({hash: hash}).toArray(function(err, results) {
    if(err) {
      return callback(err);
    }
    var block = results[0];
    if(block) {
      block.hash = toBuffer(block.hash);
      block.prev_hash = toBuffer(block.prev_hash);
    }
    callback(undefined, block);
  });
};



ChainSync.prototype.run = function() {
  var self = this;
  //peerman.addPeer(new Peer('10.132.63.139', 22556));
  var p2pPort = bitcore.networks[this.netname].defaultClientPort;
  config[this.netname].peers.forEach(function(peerHost){
    self.peerman.addPeer(new Peer(peerHost, p2pPort));    
  });
/*  this.peerman.addPeer(new Peer('115.29.186.22', p2pPort));
  this.peerman.addPeer(new Peer('192.155.84.55', p2pPort)); */
  this.peerman.on('connection', function(conn) {
    conn.on('inv', self.handleInv.bind(self));
    conn.on('block', self.handleBlock.bind(self));
    conn.on('tx', self.handleTx.bind(self));
    conn.on('version', self.handleVersion.bind(self));
    conn.on('verack', self.handleVerAck.bind(self));
  });

  function syncJob() {
    setInterval(function() {
      if(!self.fifoAddBlock.isCalling()) {
	self.getBlocks();
      } else {
	//console.info('pushing block');
      }
    }, 10000);
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
      if(tx.outs[i].s) {
	var script = new Script(tx.outs[i].s);
	txOut.addrs = script.getAddrStr(self.netname);
      }
      txOut.v = util.valueToBigInt(out.v).toString();
      return txOut;
    });
    return txObj;
  });

  this.fifoAddBlock.unshift(
    this.addBlock.bind(this),
    [obj], function(err) {
      if(err) {
	console.error(err);
      }
    });
};

ChainSync.prototype.handleTx = function(info) {
  var tx = info.message.tx.getStandardizedObject();
  //console.log('** TX Received **', tx);
};

ChainSync.prototype.handleInv = function(info) {
//    console.log('** Inv **');
//    console.log(info.message);
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
/*  var genesisBlock = helper.clone(bitcore.networks[this.netname].genesisBlock);
  var inv = {type: 2, hash:genesisBlock.hash};
  console.info('get inv', inv);
  info.conn.sendGetData([inv]); */
};

ChainSync.prototype.getBlocks = function() {
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
  this.getLatestBlock(function(err, latestBlock) {
    if(latestBlock) {
      if(latestBlock.height >= self.start_height) {
	return;
      }
      var gHash = helper.reverseBuffer(latestBlock.hash);
      conn.sendGetBlocks([gHash], 0);
      console.info('getting blocks starting from', gHash.toString('hex'), 'to', conn.peer.host);
    } else {
      //var gHash = buffertools.fill(new Buffer(32), 0);
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      var inv = {type: 2, hash:genesisBlock.hash};
      conn.sendGetData([inv]);
      console.info('getting geneticBlocks', 'to', conn.peer.host);
    }
  });  
};

module.exports = ChainSync;
