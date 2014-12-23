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

Store.prototype.ensureIndices = function(callback) {
  var self = this;
  var fns = [];
  function ensureIndex(colname, idx, params, conn) {
    if(!conn) {
      conn = self.dbconn;
    }
    fns.push(function(c) {
      var col = conn.collection(colname);
      col.ensureIndex(idx, params, c);
    });
  }
  
  //var blockCol = self.dbconn.collection('block');
  ensureIndex('block', {'hash': 1}, {unique: 1});
  ensureIndex('block', {'height': -1});

  ensureIndex('tx', {'hash': 1}, {unique: true});
  ensureIndex('tx', {'bhs': 1}, {});
  ensureIndex('tx', {'vin.hash': 1, 'vin.n': 1}, {});
  ensureIndex('tx', {'vin.k': 1}, {});
  ensureIndex('tx', {'vout.addrs': 1}, {});

  ensureIndex('var', {'key': 1}, {unique: 1});
  ensureIndex('sendtx', {'hash': 1},  {unique: 1});
  
  async.parallel(fns, callback);
};

Store.prototype.connect = function(callback) {
  var self = this;
  async.series([
    function(c) {
      var url = config.networks[self.netname].db.url;
      mongodb.MongoClient.connect(url, function(err, aConn) {
	if(err) return c(err);
	self.dbconn = aConn;
	c();
      });
    },
    function(c) {
      self.ensureIndices(c);
    },
    function(c) {
      setInterval(function() {
	self.queryStats(function(err) {
	  if(err) throw err;
	});
      }, 3000 + Math.random() * 1000);

      self.queryStats(c);
    }
  ], callback);
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
  blockHashes = underscore.uniq(blockHashes,
			       function(item) {return item.toString('hex');});

  if(blockHashes.length > 0) {
    blockCol.find({hash: {$in: blockHashes}}).toArray(function(err, arr) {
      var blocks = helper.dictlize(arr, function(b) {return b.hash.toString('hex');});
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

Store.prototype.getTxDict = function(txHashList, callback) {
  this.getTxes(txHashList, function(err, arr) {
    if(err) return callback(err);
    var map = helper.dictlize(arr, function(tx) {return tx.hash.toString('hex');});
    callback(err, map);
  });
};

Store.prototype.getTxDictWFields = function(txHashList, fields, callback) {
  this.getTxesWFields(txHashList, fields, function(err, arr) {
    if(err) return callback(err);
    var map = helper.dictlize(arr, function(tx) {return tx.hash.toString('hex');});
    callback(err, map);
  });
};

Store.prototype.getTxes = function(txHashList, callback) {
  txHashList = underscore.uniq(
    txHashList,
    function(item) {return item.toString('hex');});

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

Store.prototype.getTxesWFields = function(txHashList, fields, callback) {
  txHashList = underscore.uniq(
    txHashList,
    function(item) {return item.toString('hex');});

  var txCol = this.dbconn.collection('tx');
  if(txHashList.length <= 0) {
    return callback(undefined, []);
  } else if(txHashList.length == 1) {
    var query = {hash: txHashList[0]};
  } else {
    var query = {hash: {$in: txHashList}};
  }
  txCol.find(query, fields).toArray(function(err, txes) {
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
	  return self.fillInputs(tx, c);
	} else {
	  var update = {};
	  var hasChanged = false;
	  old.vin.forEach(function(input, index) {
	    if(input.addrs) {
	      update['vin.' + index + '.addrs'] = input.addrs;
	      hasChanged = true;
	    }
	    if(input.v) {
	      update['vin.' + index + '.v'] = input.v;
	      hasChanged = true;
	    }
	  });
	  if(hasChanged) {
	    return txcol.update({hash: tx.hash}, {$set: update}, c);
	  }	  
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

  var startDate = new Date();
  async.parallel(txes.map(insertTx),
	       function(err) {
		 if(err) return callback(err);
		 var times = new Date() - startDate;
		 if(times > 1000) {
		   console.warn('add txes', txes.length, 'including', newTxes.length, 'new txes', self.netname, 'takes', times/1000.0, 'secs');
		 }
		 callback();
	       });
};

Store.prototype.removeTxes = function(txes, callback) {
  var self = this;
  var txcol = this.dbconn.collection('tx');
  var txHashList = txes.map(function(tx) {return tx.hash;});
  txHashList.forEach(function(txHash) {
    console.warn('remove tx', txHash.toString('hex'));
  });
  if(txHashList.length == 0) {
    return callback();
  }
  txcol.remove({hash: {$in: txHashList}}, callback);
};

Store.prototype.removeBlock = function(block, callback) {
  var self = this;
  var col = this.dbconn.collection('tx');
  var blockCol = this.dbconn.collection('block');
  var removableTxes = [];
  var changedTxes = [];

  console.error('try removing block', block.hash?block.hash.toString('hex'):'');
  async.series([
    function(c) {
      col.find({bhs: block.hash}).toArray(function(err, txes) {
	if(err) return c(err);
	txes.forEach(function(tx) {
	  if(tx.bhs && tx.bhs.length == 1) { // Right the block.hash
	    removableTxes.push(tx);
	  } else {
	    var ridx = -1;
	    var hh = block.hash.toString('hex');
	    for(var i=0; i<tx.bhs.length; i++) {
	      if(tx.bhs[i].toString('hex') == hh) {
		ridx = i;
		break;
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

Store.prototype.verifyTx = function(tx, callback) {
  var self = this;
  function checkInput(tx, inputTxDict) {
    for(var i=0; i<tx.vin.length; i++) {
      var input = tx.vin[i];
      if(!input.hash) continue;
      var foundInput = false;
      var inputTx = inputTxDict[input.hash.toString('hex')];
      if(inputTx) {
	var output = inputTx.vout[input.n];
	if(output) {
	  foundInput = true;
	  input.addrs = output.addrs;
	  input.v = output.v;
	}
      }
      if(!foundInput) {
	return false;
      }
    }
    return true;
  }

  var inputHashes = [];
  var keys = [];
  tx.vin.forEach(function(input) {
    if(input.hash) {
      inputHashes.push(input.hash);
      keys.push(input.hash.toString('hex') + '#' + input.n);
    }
  });
  this.spentDict(keys, function(err, spentDict) {
    if(err) return callback(err);
    var hasSpent = false;
    for(var key in spentDict) {
      if(spentDict.hasOwnProperty(key)) {
	hasSpent = true;
	break;
      }
    }
    if(hasSpent) {
      // double spent
      return callback(undefined, false);
    } else {
      self.getTxDict(inputHashes, function(err, inputTxDict) {
	if(err) return callback(err);
	var verified = checkInput(tx, inputTxDict);
	callback(undefined, verified);
      });
    }
  });
};

Store.prototype.fillInputs = function(tx, callback) {
  var self = this;
  var inputHashes = [];
  tx.vin.forEach(function(input) {
    if(!input.addrs && input.hash) {
      inputHashes.push(input.hash);
    }
  });
  if(inputHashes.length == 0) return callback();

  this.getTxDictWFields(
    inputHashes,
    ['hash', 'vout'], 
    function(err, dict) {
      if(err) return callback(err);
      var changed = false;
      var update = {};
      tx.vin.forEach(function(input, index) {
	if(!input.addrs && input.hash) {
	  var inputTx = dict[input.hash.toString('hex')];
	  if(inputTx) {
	    input.addrs = inputTx.vout[input.n].addrs;
	    input.v = inputTx.vout[input.n].v;
	    update['vin.' + index + '.addrs'] = input.addrs;
	    update['vin.' + index + '.v'] = input.v;
	    changed = true;
	  } else {
	    console.error('cannot find input of', self.netname, input.k);
	  }
	}
      });
      if(changed) {
	var col = self.dbconn.collection('tx');
	return col.update({hash: tx.hash}, {$set: update}, callback);
      } else {
	return callback();
      }
    });
};


Store.prototype.spentDict = function(keys, callback) {
  var self = this;
  var col = this.dbconn.collection('tx');
  col.find({'vin.k': {$in: keys}}, ['hash', 'vin.k']).
    toArray(function(err, results) {
      
      if(err) return callback(err);
      if(results && results.length > 0) {
	var spent = {};
	results.forEach(function(tx) {
	  tx.vin.forEach(function(input) {
	    if(input.k) spent[input.k] = true;
	  });
	});
	return callback(undefined, spent);
      } else {
	return callback(undefined, {});
      }
    });
};

Store.prototype.takeInputAddresses = function(tx, callback) {
  var inputTxHashes = [];
  tx.vin.forEach(function(input) {
    if(input.hash && !input.address) {
      inputTxHashes.push(input.hash);
    }
  });

  this.getTxDict(inputTxHashes, function(err, inputTxDict) {
    if(err) return callback(err);
    tx.vin.forEach(function(input) {
      if(input.hash && !input.address) {
	var inputTx = inputTxDict[input.hash.toString("hex")];
	if(inputTx) {
	  var output = inputTx.vout[input.n];
	  if(output) {
	    input.addrs = output.addrs;
	    input.v = output.v;		
	  }
	}
      }
    });
    callback();
  });
};

Store.prototype.cleanupBlocks = function(sinceSeconds, maxSeconds, callback) {
  var self = this;
  var col = this.dbconn.collection('block');
  var currDate = new Date();
  var startObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - sinceSeconds);
  var endObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - maxSeconds);
  col.find({_id: {$gt:startObjectId, $lt:endObjectId}}).toArray(function(err, arr) {
    arr = arr || [];
    console.info('CLEANUP TASK: found blocks', arr.length, self.netname);
    var fns = [];
    arr.forEach(function(block) {
      if(!block.isMain) {
	fns.push(function(c) {
	  console.info('removing block', block.hash.toString('hex'));
	  self.removeBlock(block, c);
	});
      }
    });
    if(fns.length > 0) {
      async.series(fns, callback);
    } else {
      callback();
    }
  });
};

Store.prototype.cleanupTxes = function(sinceSecs, maxSecs, callback) {
  var self = this;
  var col = this.dbconn.collection('tx');
  var currDate = new Date();
  var startObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - sinceSecs);
  var endObjectId = mongodb.ObjectID.createFromTime(currDate.getTime()/1000 - maxSecs);
  var removableTxes = [];
  col.find({_id: {$gt:startObjectId, $lt:endObjectId}}).toArray(function(err, arr) {
    arr = arr || [];
    console.info('CLEANUP TASK: found txes', arr.length, self.netname);
    var fns = [];
    arr.forEach(function(tx) {
      if(!tx.bhs || tx.bhs.length == 0) {
	removableTxes.push(tx);
      }
    });
    if(removableTxes.length > 0) {
      self.removeTxes(removableTxes, callback);
    } else {
      callback();
    }    
  });
}
