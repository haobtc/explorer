var async = require('async');
var util = require('util');
var events = require('events');

var underscore = require('underscore');
var bitcore = require('bitcore-multicoin');
var Query = require('./Query');
var MongoStore = require('./MongoStore');
var socketio = require('socket.io');

var latestBlocks = {};

var rpcMethods = {};

function Stream(server, netnames) {
  var self = this;
  this.addrs = {};
  netnames.forEach(function(netname) {
    self.addrs[netname] = {};
  });
  this.sockets = {};

  var io = socketio(server);
  io.on('connection', function(socket) {
    var addressList = [];
    socket.on('watch', function(data) {
      data.addresses.forEach(function(addrStr) {
	var addr = new bitcore.Address(addrStr);
	if(addr.isValid()) {
	  addressList.push(addr);
	}
      });
      Query.getTXList(addressList, {}, true, function(err, txes) {
	if(err) throw err;
	console.info('send snapshot txes', txes);
	socket.emit('snapshot txes', txes);
      });

      Query.getUnspent(addressList, function(err, unspent) {
	if(err) throw err;
	console.info('send unspent', unspent);
	socket.emit('unspent', unspent);
      });

      function sendLatestBlock(netname, timeout) {
	setTimeout(function() {
	  var lb = latestBlocks[netname];
	  console.info('block', lb);
	  if(latestBlocks[netname]) {
	    socket.emit('block', lb);
	  }
	}, timeout);
      }
      var timeout = 100;
      for(var netname in latestBlocks) {
	sendLatestBlock(netname, timeout);
	timeout += 100;
      }
      self.watchAddresses(addressList, socket);
    });

    socket.on('error', function(err) {
      console.error(err);
    });
    socket.on('disconnect', function() {
      self.unwatchAddresses(addressList, socket);
    });

    socket.on('rpc call', function(rpcCall) {
      console.info('rpc call', rpcCall);
      var method = rpcCall.method;
      var fn = rpcMethods[method];
      if(fn) {
	var rpcObj = {
	  send: function(ret) {
	    socket.emit('rpc return', {
	      seq: rpcCall.seq,
	      result: ret
	    });
	  }
	};
	rpcCall.args.unshift(rpcObj);
	fn.apply(undefined, rpcCall.args);
      } else {
	console.warn('No such call', rpcCall);
      }
    });
  });

  netnames.forEach(function(netname) {
    var it = new TxIterator(netname, 'tx');
    it.on('tx', function(tx) {
      stream.onTx(tx);
    });
    it.start();

    it = new TxIterator(netname, 'mempool');
    it.on('tx', function(tx) {
      stream.onTx(tx);
    });
    it.start();

    it = new TxIterator(netname, 'archived');
    it.on('tx', function(tx) {
      stream.onTx(tx, true);
    });
    it.start();

    it = new BlockIterator(netname);
    it.on('block', function(block) {
      self.onBlock(block);
    });
    it.start();
  });

}

Stream.prototype.onBlock = function(block) {
  for(var sid in this.sockets) {
    var sock = this.sockets[sid];
    sock.emit('block', block);
  }
};

Stream.prototype.onTx = function(tx, archived) {
  var affectedSockets = {};
  var netAddrs = this.addrs[tx.network];
  tx.inputs.forEach(function(input) {
    if(!input.address) return;
    var arr = netAddrs[input.address];
    if(arr) {
      arr.forEach(function(sock) {
	affectedSockets[sock.id] = sock;
      });
    }
  });
  tx.outputs.forEach(function(output) {
    if(!output.address) return;
    var arr = netAddrs[output.address];
    if(arr) {
      arr.forEach(function(sock) {
	affectedSockets[sock.id] = sock;
      });
    }
  });
  for(var cid in affectedSockets) {
    var sock = affectedSockets[cid];
    if(archived) {
      console.info('arhived', tx.txid);
      sock.emit('archived tx', tx);
    } else {
      console.info('txx', tx.txid);
      sock.emit('tx', tx);
    }
  }
};

Stream.prototype.watchAddresses = function(addressList, socket) {
  var self = this;
  addressList.forEach(function(addr) {
    var netname = addr.network().name;
    var addrStr = addr.toString();
    var netAddrs = self.addrs[netname];
    var arr = netAddrs[addrStr];
    if(arr) {
      arr.push(socket);
    } else {
      arr = [socket];
      netAddrs[addrStr] = arr;
    }
  });
  this.sockets[socket.id] = socket;
};

Stream.prototype.unwatchAddresses = function(addressList, socket) {
  var self = this;
  delete this.sockets[socket.id];

  addressList.forEach(function(addr) {
    var netname = addr.network().name;
    var addrStr = addr.toString();
    var netAddrs = self.addrs[netname];
    var arr = netAddrs[addrStr];
    if(arr) {
      arr = underscore.reject(arr, function(sock) { return sock == socket; });
      if(arr.length > 0) {
	netAddrs[addrStr] = arr;
      } else {
	delete netAddrs[addrStr];
      }
    }
  });
};

var stream = null;
module.exports.createStream = function(server, netnames){
  stream = new Stream(server, netnames);
};
/**
 * TxIterator through new txes
 * 
 */
function TxIterator(netname, colname) {
  this.colname = colname;
  this.netname = netname;
  this.txId = null;
}
util.inherits(TxIterator, events.EventEmitter);

TxIterator.prototype.store = function() {
  var store = MongoStore.stores[this.netname];
  return store;
};

TxIterator.prototype.nextTx = function(callback) {
  var self = this;
  var col = this.store().dbconn.collection(this.colname);
  if(!this.txId) {
    // Get the latest tx id first
    col.find().sort({_id: -1}).limit(1).toArray(function(err, vals) {
      if(err) return callback(err);
      if(vals.length > 0) {
	var tx = vals[0];
	self.txId = tx._id;
      }
      callback();
    });
  } else {
    // Get the latest tx id first
    col.find({_id: {$gt: this.txId}}).sort({_id: 1}).limit(1).toArray(function(err, vals) {
      if(err) return callback(err);
      if(vals.length > 0) {
	var tx = vals[0];
	self.txId = tx._id;
	callback(undefined, tx);
      } else {
	callback();
      }
    });    
  }  
};

TxIterator.prototype.start = function() {
  var self = this;
  async.doWhilst(function(c) {
    self.nextTx(function(err, tx) {
      if(err) return c(err);
      if(tx) {
	Query.txListToJSON(self.store(), [tx], function(err, txList) {
	  if(err) return c(err);
	  if(txList.length > 0) {
	    self.emit('tx', txList[0]);
	  }
	  setTimeout(c, 0);
	});
      } else {
	setTimeout(c, 3000);
      }
    });
  }, function() {
    return true;
  }, function(err) {
    if(err) throw err;
  });
};

/**
 * BlockIterator through new txes
 * 
 */
function BlockIterator(netname) {
  this.netname = netname;
  this.blockId = null;
}
util.inherits(BlockIterator, events.EventEmitter);

BlockIterator.prototype.store = function() {
  var store = MongoStore.stores[this.netname];
  return store;
};

BlockIterator.prototype.nextBlock = function(callback) {
  var self = this;
  var col = this.store().dbconn.collection('block');
  if(!this.blockId) {
    // Get the latest block id first
    col.find().sort({_id: -1}).limit(1).toArray(function(err, vals) {
      if(err) return callback(err);
      if(vals.length > 0) {
	var block = vals[0];
	block.network = self.netname;
	block = Query.handleBlock(block);
	latestBlocks[self.netname] = block;
	self.blockId = block._id;
      }
      callback();
    });
  } else {
    // Get the latest block id first
    col.find({_id: {$gt: this.blockId}}).sort({_id: 1}).limit(1).toArray(function(err, vals) {
      if(err) return callback(err);
      if(vals.length > 0) {
	var block = vals[0];
	self.blockId = block._id;
	block.network = self.netname;
	block = Query.handleBlock(block);
	latestBlocks[self.netname] = block;
	callback(undefined, block);
      } else {
	callback();
      }
    });    
  }  
};

BlockIterator.prototype.start = function() {
  var self = this;
  async.doWhilst(function(c) {
    self.nextBlock(function(err, block) {
      if(err) return c(err);
      if(block) {
	self.emit('block', block);
	setTimeout(c, 0);
      } else {
	setTimeout(c, 3000);
      }
    });
  }, function() {
    return true;
  }, function(err) {
    if(err) throw err;
  });
};

// RPC methods
module.exports.addRPC = function(method, fn) {
  rpcMethods[method] = fn;
};
module.exports.addRPC('hello', function(rpc, a, b) {
  rpc.send('xx' + a + b);
});
