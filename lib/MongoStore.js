var underscore = require('underscore');
var mongodb = require('mongodb');
var async = require('async');
var events = require('events');
var helper = require('./helper');

var config = require('./config');
var stores = {};
module.exports.stores = stores;

module.exports.initialize = function(netnames, callback, complete) {
  var tasks = netnames.map(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    return function(c) {
      store.connect(function(err, conn) {
	if(err) return c(err);
	if(typeof callback == 'function') callback(err, netname);
	c(err, netname);
      });
    };
  });
  async.parallel(tasks, complete);
};

function Store(netname) {
  var self = this;
  this.netname = netname;
  this.events = new events.EventEmitter();
  this.stats = {
    maxHeight: -1,
    tipTimestamp: 0
  };
}

Store.prototype.queryStats = function(callback) {
  var self = this;
  this.getTipBlock(function(err, tip) {
    if(err) return callback(err);
    if(tip) {
      self.stats.maxHeight = tip.height;
      self.stats.tipTimestamp = tip.timestamp;
      return callback();
    } else {
      return callback();
    }
  });
}

Store.prototype.on = function(event, listener) {
  this.events.on(event, listener);
};

Store.prototype.emit = function() {
  this.events.emit.apply(this.events, arguments);
};

Store.prototype.getVar = function(key, callback) {
  var col = this.dbconn.collection('var');
  col.findOne({key: key}, callback);
};

Store.prototype.saveVar = function(v, callback) {
  delete v._id;
  var col = this.dbconn.collection('var');
  col.findAndModify({key: v.key}, [], {$set: v}, {upsert: true, "new": true},
		    callback);

};

Store.prototype.connect = function(callback) {
  var self = this;
  var url = config.networks[this.netname].db.url;
  mongodb.MongoClient.connect(url, function(err, aConn) {
    if(err) {
      console.error(err);
      if(typeof callback == 'function') return callback(err);
    }
    self.dbconn = aConn;
    
    function nf(err) {if(err){console.error(err);}}

    var blockCol = self.dbconn.collection('block');
    blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    blockCol.ensureIndex({'height': -1}, nf);

    var txCol = self.dbconn.collection('tx');
    txCol.ensureIndex({'hash': 1}, {unique: true}, nf);
    //txCol.ensureIndex({'bhash': 1}, {}, nf);
    txCol.ensureIndex({'bhs': 1}, {}, nf);
    txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1},
		      {}, nf);
    txCol.ensureIndex({'vin.k': 1}, {}, nf);
    //txCol.ensureIndex({'vout.spt': 1}, {}, nf);
    txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var varCol = self.dbconn.collection('var');
    varCol.ensureIndex({'key': 1}, {unique: 1}, nf);

    var sendTxCol = self.dbconn.collection('sendtx');
    sendTxCol.ensureIndex({'hash': 1},  {unique: 1}, nf);

    self.queryStats(function(err) {
      if(err) callback(err);
      callback(undefined, self.dbconn);      
    });

    setInterval(function() {
      self.queryStats(function(err) {
	if(err) throw err;
      });
    }, 3000 + Math.random() * 1000);
  });
};

Store.prototype.getBlock = function(hash, callback) {
  var self = this;
  if(!hash) return callback();

  var col = this.dbconn.collection('block');
  col.findOne({hash: hash}, function(err, block) {
    if(err) {
      return callback(err);
    }
    if(block) {
      block.hash = helper.toBuffer(block.hash);
      block.prev_hash = helper.toBuffer(block.prev_hash);
      if(block.next_hash) {
	block.next_hash = helper.toBuffer(block.next_hash);
      }
    }
    callback(undefined, block);
  });
};

Store.prototype.getTipBlock = function(callback) {
  var self = this;
  var col = this.dbconn.collection('var');
  col.findOne({key: 'tip'}, function(err, obj) {
    if(err) return callback(err);
    if(obj) {
      self.getBlock(obj.blockHash, callback);
    } else {
      callback();
    }
  });
};

Store.prototype.setTipBlock = function(block, callback) {
  var col = this.dbconn.collection('var');
  col.update({'key': 'tip'}, {$set: {blockHash: block.hash}},
	     {upsert: true}, callback);
};

Store.prototype.saveBlock = function(block, callback) {
  var self = this;
  var col = this.dbconn.collection('block');
  col.save(block, callback);
};


Store.prototype.getBlocks = function(txes, callback) {
  var blockHashes = [];
  var blockCol = this.dbconn.collection('block');
  txes.forEach(function(tx) {
    if(tx.bhs && tx.bhs.length > 0) {
      tx.bhs.forEach(function(bh) {
	blockHashes.push(bh);
      });
    }
  });
  blockHashes = underscore.uniq(blockHashes);

  if(blockHashes.length > 0) {
    blockCol.find({hash: {$in: blockHashes}}).toArray(function(err, arr) {
      var blocks = {};
      arr.forEach(function(block) {
	blocks[block.hash.toString('hex')] = block;
      });
      txes.forEach(function(tx) {
	tx.blocks = [];
	if(tx.bhs) {
	  tx.bhs.forEach(function(bh) {
	    tx.blocks.push(blocks[bh.toString('hex')]);
	  });
	}
      });
      return callback(err, txes);
    });
  } else {
    return callback(undefined, txes);
  }
};

Store.prototype.getTxes = function(txHashList, callback) {
  var txCol = this.dbconn.collection('tx');

  if(txHashList.length <= 0) {
    return callback(undefined, []);
  } else if(txHashList.length == 1) {
    var query = {hash: txHashList[0]};
  } else {
    var query = {hash: {$in: txHashList}};
  }
  txCol.find(query).toArray(function(err, txes) {
    if(err) return callback(err);
    txes.forEach(function(tx) {
      tx.hash = helper.toBuffer(tx.hash);
    });
    return callback(err, txes);
  });
};

Store.prototype.getTx = function(hash, callback) {
  return this.getTxes([hash], function(err, arr){
    if(err) return callback(err);
    if(arr.length == 0) {
      return callback(err, null);
    } else {
      return callback(err, arr[0]);
    }
  });
};

Store.prototype.addTxes = function(txes, callback) {
  var self = this;
  var txcol = this.dbconn.collection('tx');
  var newTxes = [];

  function insertTx(tx) {
    return function(c) {
      function checkNew(err, old) {
	if(err) return c(err);
	if(!old) {
	  newTxes.push(tx);
	}
	return c();
      }

      delete tx.bhs;
      delete tx.bis;
      if(tx.bhash) {
	var bhash = tx.bhash;
	var bindex = tx.bidx;
	delete tx.bhash;
	delete tx.bidx;
	txcol.findAndModify({hash: tx.hash}, [],
			    {
			      $push: {bhs: bhash, bis: bindex},
			      $set: tx
			    },
			    {upsert: true, new: false},
			   checkNew);
      } else {
	txcol.findAndModify({hash: tx.hash}, [],
			    {$set: tx},
			    {upsert: true},
			    checkNew);
      }
    };
  }

  async.parallel(txes.map(insertTx),
	       function(err) {
		 if(err) return callback(err);
		 self.incSpent(newTxes);
		 callback(err);
	       });
};

Store.prototype.removeTxes = function(txes, callback) {
  var self = this;
  var txcol = this.dbconn.collection('tx');
  var txHashList = txes.map(function(tx) {return tx.hash;});
  console.info('remove txes', txHashList);
  txcol.remove({hash: {$in: txHashList}}, function(err) {
    if(err) return callback(err);
    self.decSpent(txes);
    callback();
  });
};

Store.prototype.removeBlock = function(block, callback) {
  var self = this;
  var col = this.dbconn.collection('tx');
  var blockCol = this.dbconn.collection('block');
  var removableTxes = [];
  var changedTxes = [];

  async.series([
    function(c) {
      col.find({bhs: block.hash}).toArray(function(err, txes) {
	if(err) return c(err);
	txes.forEach(function(tx) {
	  if(tx.bhs.length == 1) { // Right the block.hash
	    removableTxes.push(tx);
	  } else {
	    var ridx = -1;
	    var hh = block.hash.toString('hex');
	    for(var i=0; i<tx.bhs.length; i++) {
	      if(tx.bhs[i].hash.toString('hex')) {
		ridx = i;
	      }
	    }
	    if(ridx >= 0) {
	      tx.bhs.splice(ridx);
	      tx.bis.splice(ridx);
	      changedTxes.push(tx);
	    }
	  }
	});
	console.info('changedTxes', changedTxes.length);
	c();
      });
    },
    function(c) {
      if(removableTxes.length == 0) return c();
      self.removeTxes(removableTxes, c);
    },
    function(c) {
      if(changedTxes.length == 0) return c();
      async.parallel(changedTxes.map(function(tx) {
	return function(c) {
	  col.save(tx, c);
	};
      }), c);
    }, 
    function(c) {
      blockCol.remove({hash: block.hash}, c);
    }],
	       callback);
};

// Extra tasks on modification of txes
Store.prototype.incSpent = function(txes) {
  var col = this.dbconn.collection('tx');
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	var setV = {};
	setV['vout.' + input.n + '.spt'] = 1;
	col.update({'hash': input.hash}, {$inc:setV}, function(err) {
	  if(err) throw err;
	});
      }
    });
  });
};

Store.prototype.decSpent = function(txes) {
  var col = this.dbconn.collection('tx');
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	var setV = {};
	setV['vout.' + input.n + '.spt'] = -1;
	col.update({'hash': input.hash}, {$inc:setV}, function(err) {
	  if(err) throw err;
	});
      }
    });
  });
};


Store.prototype.verifyTx = function(tx, callback) {
  function checkInput(tx, arr) {
    var inputTxDict = helper.dictlize(arr,
				      function(t) {return t.hash;});
    for(var i=0; i<tx.vin.length; i++) {
      var input = tx.vin[i];
      if(!input.hash) continue;
      var srctx = inputTxDict[input.hash];
      if(!srctx) return false;
      var output = srctx.vout[input.n];
      if(!output || output.spt) return false;
    }
    return tx.vout.length > 0;
  }
  var inputHashes = [];

  tx.vin.forEach(function(input) {
    if(input.hash) {
      inputHashes.push(input.hash);
    }
  });
  inputHashes = underscore.uniq(
    inputHashes,
    function(item) {return item.toString('hex');});
  if(inputHashes.length == 0) return callback(undefined, true);
  
  var col = this.dbconn.collection("tx");
  col.find({hash: {$in: inputHashes}}).toArray(function(err, arr) {
    if(err) return callback(err);
    if(arr.length == 0) return callback(undefined, false);
    var verified = checkInput(tx, arr);
    callback(undefined, verified);
  });
};



