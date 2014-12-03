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
      var idxUrl = config.networks[self.netname].db.idxUrl;
      if(!idxUrl || idxUrl == config.networks[self.netname].db.url) {
	self.idxconn = self.dbconn;
	return c();
      } else {
	mongodb.MongoClient.connect(idxUrl, function(err, aConn) {
	  if(err) return c(err);
	  self.idxconn = aConn;
	  return c();
	});
      }
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
    var map = helper.dictlize(arr, function(tx) {return tx.hash;});
    callback(err, map);
  });
};

Store.prototype.getTxInfoDict = function(txHashList, callback) {
  var self = this;
  if(!txHashList || txHashList.length == 0) {
    return callback(undefined,  {});
  }
  txHashList = underscore.uniq(
    txHashList,
    function(item) {return item.toString('hex');});

  var col = this.idxconn.collection('txinfo');

  var q = txHashList.map(function(txhash) {return 'new HexData(0, "'+txhash.toString('hex')+'")';});
  //console.info('ff', {$in: '[' + q.join(',') + ']'});
  console.info(new Date(), 'xxx', 'finding from idxconn', txHashList.length);
  col.find({_id: {$in: txHashList}}).toArray(function(err, arr) {
    console.info(new Date(), 'xxx', 'found from idxconn');
    if(err) return callback(err);
    //arr = underscore.filter(arr, function(txinfo) {return !!txinfo.vals});
    var dict = helper.dictlize(arr, function(txinfo) {return txinfo._id});
    var noHashList = underscore.filter(txHashList, function(hash) {
      return !dict[hash] || !dict[hash].vals;
    });
    console.info(new Date(), 'xxx', 'no hash list');
    if(noHashList.length > 0) {
      self.getTxes(noHashList, function(err, arr) {
	if(err) return callback(err);
	arr.forEach(function(tx) {
	  var info = self.txInfo(tx);
	  if(dict[tx.hash]) {
	    dict[tx.hash].addrs = info.addrs;
	    dict[tx.hash].vals = info.vals;
	  } else {
	    dict[tx.hash] = info;
	  }
	});
	return callback(err, dict);
      });      
    } else {
      return callback(err, dict);
    }
  });
};

Store.prototype.txInfo = function(tx) {
  var info = {spt: {}, addrs: [], vals: []};
  var update = {};
  tx.vout.forEach(function(output, idx) {
    if(output.addrs) {
      info.addrs.push(output.addrs.join(','));
    } else {
      info.addrs.push('');
    }
    if(output.v) {
      info.vals.push(output.v);
    } else {
      info.vals.push('');
    }

    if(output.spt > 0) {
      info.spt[idx] = output.spt;
      update['spt.' + idx] = output.spt;
    }
  });
  update['addrs'] = info.addrs;
  var col = this.idxconn.collection('txinfo');
  col.update({_id: tx.hash}, {$set: update}, {upsert: true}, function(err) {if(err) throw err;});
  return info;
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

  var startDate = new Date();
  async.parallel(txes.map(insertTx),
	       function(err) {
		 if(err) return callback(err);
		 var insertTimes = new Date() - startDate;
		 self.incSpent(newTxes, function(err) {
		   if(err) return callback(err);
		   var incTimes = new Date() - startDate;
		   self.updateAddrs(newTxes, function(err) {
		     if(err) return callback(err);
		     var times = new Date() - startDate;
		     if(times > 1000) {
		       console.warn('add txes', txes.length, self.netname, 'takes', times/1000.0, 'secs', 'insert', insertTimes/1000.0, 'secs', 'inc', incTimes/1000.0, 'secs');
		     }
		     callback(err);
		   });
		 });
	       });
};

Store.prototype.removeTxes = function(txes, callback) {
  var self = this;
  var txcol = this.dbconn.collection('tx');
  var txHashList = txes.map(function(tx) {return tx.hash;});
  txHashList.forEach(function(txHash) {
    console.warn('remove tx', txHash.toString('hex'));
  });
  txcol.remove({hash: {$in: txHashList}}, function(err) {
    if(err) return callback(err);
    self.decSpent(txes, function(err) {
      if(err) return callback(err);
      var infocol = self.idxconn.collection('txinfo');
      infocol.remove({_id: {$in: txHashList}}, callback);
    });
  });
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

// Extra tasks on modification of txes
Store.prototype.incSpent = function(txes, callback) {
  var col = this.idxconn.collection('txinfo');
  var fns = [];
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	fns.push(function(c) {
	  var setV = {};
	  //setV['vout.' + input.n + '.spt'] = 1;
	  setV['spt.' + input.n] = 1;
	  col.update({'_id': input.hash}, {$inc:setV}, {upsert: true}, c);
	});
      }
    });
  });
  if(fns.length > 0) {
    async.parallel(fns, callback);
  } else {
    callback();
  }
};

Store.prototype.updateAddrs = function(txes, callback) {
  var col = this.idxconn.collection('txinfo');
  var fns = [];
  txes.forEach(function(tx) {
    var addrs = [];
    var vals = [];
    tx.vout.forEach(function(output, outidx) {
      if(output.addrs) {
	addrs.push(output.addrs.join(','));
      } else {
	addrs.push('');
      }
      if(output.v) {
	vals.push(output.v);
      } else {
	vals.push('');
      }
    });
    fns.push(function(c) {
      col.update({'_id': tx.hash},
		 {$set:{
		   addrs:addrs,
		   vals: vals},
		 },
		 {upsert: true}, c);
    });
  });
  if(fns.length > 0) {
    async.parallel(fns, callback);
  } else {
    callback();
  }
};

Store.prototype.decSpent = function(txes, callback) {
  var col = this.idxconn.collection('txinfo');
  var txcol = this.dbconn.collection('tx');
  var fns = [];
  txes.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && input.n >= 0) {
	fns.push(function(c) {
	  var setV = {};
	  //setV['vout.' + input.n + '.spt'] = -1;
	  setV['spt.' + input.n] = -1;
	  col.update({'_id': input.hash}, {$inc:setV}, c);
	});
	fns.push(function(c) {
	  var setV = {};
	  setV['vout.' + input.n + '.spt'] = -1;
	  var query = {hash: input.hash};
	  query['vout.' + input.n + '.spt'] = {$gt: 0};
	  txcol.update(query, {$inc:setV}, c);
	});
      }
    });
  });
  if(fns.length > 0) {
    async.parallel(fns, callback);
  } else {
    callback();
  }
};


Store.prototype.verifyTx = function(tx, callback) {
  function checkInput(tx, inputTxDict) {
    for(var i=0; i<tx.vin.length; i++) {
      var input = tx.vin[i];
      if(!input.hash) continue;
      var txInfo = inputTxDict[input.hash];
      if(!txInfo) return false;

      if(txInfo.spt && txInfo.spt[input.n]) return false;
      if(txInfo.addrs && txInfo.addrs[input.n]) {
	input.addrs = txInfo.addrs[input.n].split(',');
      }
      if(txInfo.vals && txInfo.vals[input.n]) {
	input.v = txInfo.vals[input.n];
      }
    }
    return tx.vout.length > 0;
  }

  var inputHashes = [];
  tx.vin.forEach(function(input) {
    if(input.hash) {
      inputHashes.push(input.hash);
    }
  });
  this.getTxInfoDict(inputHashes, function(err, dict) {
    if(err) return callback(err);
    var verified = checkInput(tx, dict);
    callback(undefined, verified);
  });
};

Store.prototype.takeInputAddresses = function(tx, callback) {
  var inputTxHashes = [];
  tx.vin.forEach(function(input) {
    if(input.hash && !input.address) {
      inputTxHashes.push(input.hash);
    }
  });

  this.getTxInfoDict(inputTxHashes, function(err, inputTxInfos) {
    if(err) return callback(err);
    tx.vin.forEach(function(input) {
      if(input.hash && !input.address) {
	var txInfo = inputTxInfos[input.hash];
	if(txInfo) {
	  input.addrs = txInfo.addrs[input.n].split(',');
	}
      }
    });
    callback();
  });
};
