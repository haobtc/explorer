var MongoStore = require('./MongoStore');
var mongodb = require('mongodb');
var _ = require('underscore');
var async = require('async');
var Defer = require('./Defer');
var bitcore = require('../alliances/bitcore/bitcore');
var helper = require('./helper');

function getStoreDict(addressList) {
  var storeDict = {};
  addressList.forEach(function(addr) {
    addr.possibleNetworks().forEach(function(network) {
      var netname = network.name;
      var s = storeDict[netname];
      if(s) {
	s.arr.push(addr.toString());
      } else {
	storeDict[netname] = {
	  netname: netname,
	  arr: [addr.toString()],
	  store: MongoStore.stores[netname]
	}
      }
    });
  });
  return storeDict;
};

function getBlocks(store, txList, callback) {
  var blockHashes = [];
  txList.forEach(function(tx) {
    if(tx.bhash) {
      blockHashes.push(tx.bhash);
    }
  });

  var col = store.dbconn.collection('block');
  col.find({hash: {$in: blockHashes}}).toArray(function(err, blocks) {
    if(err) return callback(err, undefined);
    var blockObjs = {};
    blocks.forEach(function(block) {
      blockObjs[block.hash.toString('hex')] = block;
    });
    callback(undefined, blockObjs);
  });
}

function getUnspentFromChain(store, addressList, callback) {
  var txcol = store.dbconn.collection('tx');
  var addrDict = {};
  addressList.forEach(function(addr) {
    addrDict[addr] = true;
  });

  txcol.find({'vout.addrs': {$in: addressList}}).toArray(function(err, txes) {
    if(err) {
      return callback(err);
    }
    var outputs = [];
    getBlocks(store, txes, function(err, blockObjs) {
      if(err) return callback(err);
      txes.forEach(function(tx) {
	var block = blockObjs[tx.bhash.toString('hex')];
	tx.vout.forEach(function(output, vidx) {

	  for(var i=0; i<output.addrs.length; i++) {
	    if(!!addrDict[output.addrs[i]] &&
	       !output.spt) {
	      var obj = {
		txid: tx.hash.toString('hex'),
		amount: helper.satoshiToNumberString(output.v),
		amountSatoshi: output.v.toString(),
		address: output.addrs[i],
		network: store.netname,
		vout: vidx,
		scriptPubKey: output.s.toString('hex')
	      }
	      if(block) {
		obj.confirmations = Math.max(0, store.max_height - block.height + 1);
		obj.time = block.timestamp;
	      } else {
		obj.confirmations = 0;
		obj.time = Math.round(new Date().getTime()/1000);
	      }
	      obj.key = obj.txid + '#' + obj.vout;
	      outputs.push(obj);
	      break;
	    }
	  }
	});
      });
      filterSpent(store, outputs, callback);
      //callback(undefined, outputs);
    }); // end of getBlocks
  });
}

function getUnspentFromMempool(store, addressList, callback) {
  var txcol = store.dbconn.collection('mempool');
  var addrDict = {};
  addressList.forEach(function(addr) {
    addrDict[addr] = true;
  });

  var spentOutputs = {};
  txcol.find({'vout.addrs': {$in: addressList}}).toArray(function(err, txes) {
    if(err) {
      return callback(err);
    }
    var outputs = [];
    txes.forEach(function(tx) {
      tx.vin.forEach(function(input, vidx) {
	var key = input.k || (input.hash.toString('hex') + '#' + input.n);
	spentOutputs[key] = true;
      });
      tx.vout.forEach(function(output, vidx) {
	for(var i=0; i<output.addrs.length; i++) {
	  if(!!addrDict[output.addrs[i]]) {
	    var obj = {
	      txid: tx.hash.toString('hex'),
	      amount: helper.satoshiToNumberString(output.v), 
	      amountSatoshi: output.v.toString(),
	      address: output.addrs[i],
	      network: store.netname,
	      vout: vidx,
	      scriptPubKey: output.s.toString('hex'),
	      confirmations: 0
	    }
	    obj.key = obj.txid + '#' + obj.vout;
	    obj.time = Math.round(tx._id.getTimestamp().getTime()/1000)
	    outputs.push(obj);
	    break;
	  }
	}
      });
    });
    callback(undefined, outputs, spentOutputs);
  });
}

function filterSpent(store, outputs, callback) {
  if(!outputs || outputs.length == 0) {
    return callback(undefined, []);
  }
  var keys = outputs.map(function(output) { return output.key});
  var txcol = store.dbconn.collection('tx');
  txcol.find(
    {'vin.k': {$in: keys}}, ['hash', 'vin.k'])
    .toArray(
      function(err, results) {
	if (err) return callback(err);
	if(results && results.length > 0) {
	  var spent = [];
	  results.forEach(function(tx) {
	    tx.vin.forEach(function(input) {
	      if(input.k) spent[input.k] = true;
	    });
	  });
	  var unspentOutputs = [];
	  outputs.forEach(function(output) {
	    if(!spent[output.key]) {
	      unspentOutputs.push(output);
	    }
	    //delete output.key;
	  });
	  callback(undefined, unspentOutputs);
	} else {
	  callback(undefined, outputs);
	}
      });
}

module.exports.getUnspent = function(addressList, callback) {
  var storeDict = getStoreDict(addressList);

  function getUnspent(netname) {
    return function(cb) {
      var s = storeDict[netname];
      var unspent = [];
      async.series([
	function(c) {
	  getUnspentFromChain(s.store, s.arr, function(err, outputs) {
	    if(err) return c(err);
	    outputs.forEach(function(uspt) {
	      unspent.push(uspt);
	    });
	    c();
	  });
	},
	function(c) {
	  getUnspentFromMempool(s.store, s.arr, function(err, outputs, spentInMemPool) {
	    if(err) return c(err);
	    var tmpUnspent = [];
	    var keyDict = {};
	    unspent.forEach(function(uspt) {
	      if(!keyDict[uspt.key]) {
		keyDict[uspt.key] = true;
		if(!spentInMemPool[uspt.key]) {
		  delete uspt.key;
		  tmpUnspent.push(uspt);
		}
	      }
	    });
	    outputs.forEach(function(uspt) {
	      if(!keyDict[uspt.key]) {
		keyDict[uspt.key] = true;
		if(!spentInMemPool[uspt.key]) {
		  delete uspt.key;
		  tmpUnspent.push(uspt);
		}
	      }
	    });
	    unspent = tmpUnspent;
	    c();
	  });
	}], function(err) {
	  if(err) return cb(err);
	  cb(undefined, unspent);
	});
    };
  };
  var tasks = [];
  for(var netname in storeDict) {
    tasks.push(getUnspent(netname));
  }
  async.parallel(tasks, function(err, results) {
    if(err) return callback(err);
    var outputs = _.flatten(results, true);
    callback(undefined, outputs);
  });
};

module.exports.txListToJSON = function(store, txList, callback) {
  var txDetails;
  var blockObjs;
  var inputTxDict = {};
  async.series([
    function(c) {
      inputTxes(store, txList, function(err, vals) {
	if(err) return c(err);
	inputTxDict = vals;
	c();
      });
    },
    function(c) {
      getBlocks(store, txList, function(err, vals) {
	if(err) return c(err);
	blockObjs = vals;
	c();
      });
    },
    function(c) {
      txDetails = txList.map(function(tx, vindex) {
	var block;
	if(tx.bhash) {
          block = blockObjs[tx.bhash.toString('hex')];
	}
        var txObj = {
	  ts: tx._id.getTimestamp().getTime(),
	  id: tx._id.toString(),
          network: store.netname,
          txid: tx.hash.toString('hex'),
	  inputs: [],
	  outputs: [],
        };
	if(block) {
          txObj.blockhash = block.hash.toString('hex');
          txObj.blocktime = block.timestamp;
	  txObj.blockheight = block.height;
          txObj.time = block.timestamp;
	  txObj.confirmations = Math.max(0, store.max_height - block.height + 1);
	} else {
	  txObj.time = Math.round(tx._id.getTimestamp().getTime()/1000);
	  txObj.confirmations = 0;
	}

	txObj.inputs = tx.vin.map(function(input, idx) {
	  var iObj = {script: input.s.toString('hex'),
		      amount:'0',
		      address:''};
	  if(input.hash) {
	    iObj.hash = input.hash.toString('hex');
	    iObj.vout = input.n;	      
	    if(input.addrs) {
	      iObj.address = input.addrs.join(',');
	      iObj.amount = helper.satoshiToNumberString(input.v);
	      iObj.amountSatoshi = input.v.toString();
	    } else {
	      var inputTx = inputTxDict[input.hash.toString('hex')];
	      if(inputTx) {
		var output = inputTx.vout[input.n];
		iObj.address = output.addrs.join(',');
		iObj.amount = helper.satoshiToNumberString(output.v);
		iObj.amountSatoshi = output.v.toString();
		var txcol = store.dbconn.collection('tx');

		var up = {};
		up['vin.' + idx + '.addrs'] = output.addrs;
		up['vin.' + idx + '.v'] = output.v;
		txcol.update({'hash': tx.hash}, {$set: up}, function(err) {if(err) throw err;});
	      }
	    }
	  }
	  return iObj;
	});
	txObj.outputs = tx.vout.map(function(output) {
	  return {
            script: output.s.toString('hex'),
	    address: output.addrs.join(','),
	    amount: helper.satoshiToNumberString(output.v),
	    amountSatoshi: output.v.toString(),
	    spent: !!output.spt
	  };
	});
	return txObj;
      });
      c();
    }
  ], function(err) {
    callback(err, txDetails);
  });
}


function inputTxes(store, txList, callback) {
  var inputTxHashList = [];
  txList.forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash && !input.addrs) {
	inputTxHashList.push(input.hash);
      }
    });
  });
  var inputTxDict = {};
  store.getTxes(inputTxHashList, function(err, vals) {
    if(err) return callback(err);
    vals.forEach(function(tx) {
      inputTxDict[tx.hash.toString('hex')] = tx;
    });
    callback(undefined, inputTxDict);
  });
};

module.exports.getTxDetails = function(store, hashList, callback) {
  var col = store.dbconn.collection('tx');
  var txList;
  var txDetails;

  async.series([
    function(c) {
      store.getTxes(hashList, function(err, vals) {
	txList = vals;
	c();
      });
    },
    function(c) {
      module.exports.txListToJSON(store, txList, function(err, vals) {
	if(err) return c(err);
	txDetails = vals;
	c();
      });
    }
  ], function(err) {
    if(err && err instanceof Skip) {
      err = null;
    }
    if(err) return callback(err);
    callback(undefined, txDetails);
  });
};

// Get TxList via ID
function getTxListSinceID(store, since_id, colname, callback) {
  var col = store.dbconn.collection(colname);
  if(since_id) {
    var objid = mongodb.ObjectID(since_id);
    col.find({_id: {$gt: objid}}).sort({_id: 1}).limit(10).toArray(callback);
  } else {
    col.find().sort({_id: -1}).limit(10).toArray(function(err, arr){
      if(err) return callback(err);
      arr = arr.reverse();
      callback(err, arr);
    });
  }
}

module.exports.getTxDetailsSinceID = function(store, since_id, callback) {
  var txList = [];
  var txDetails = [];
  async.series([
    function(c) {
      getTxListSinceID(store, since_id, 'tx', function(err, arr) {
	if(err) return c(err);
	arr.forEach(function(tx) {
	  txList.push(tx);
	});
	c();
      });
    },
    function(c) {
      getTxListSinceID(store, since_id, 'mempool', function(err, arr) {
	if(err) return c(err);
	arr.forEach(function(tx) {
	  txList.push(tx);
	});
	c();
      });
    },
    function(c) {
      module.exports.txListToJSON(store, txList, function(err, vals) {
	if(err) return c(err);
	txDetails = vals || [];
	c();
      });
    }
  ], function(err) {
    if(err) return callback(err);
    callback(err, txDetails);
  });
};

// Get TXList
function getTXListFromOutput(store, query, requireDetail, colname, callback) {
  var col = store.dbconn.collection(colname);
  if(requireDetail) {
    col.find(query).toArray(callback);
  } else {
    col.find(query, ['hash', 'vout']).toArray(callback);
  }
};

function getTXListFromInput(store, query, requireDetail, colname, callback) {
  var col = store.dbconn.collection(colname);
  if(requireDetail) {
    col.find(query).toArray(callback);
  } else {
    col.find(query, ['hash']).toArray(callback);
  }
};

module.exports.getTXList = function(addressList, idQuery, requireDetail, callback) {
  var storeDict = getStoreDict(addressList);
  function getTXList(netname) {
    return function(cb) {
      var s = storeDict[netname];
      var txList = [];
      var keys = [];
      var sDict = {};
      s.arr.forEach(function(a) {
	sDict[a] = true;
      });
      async.series([
	function(c) {
	  var query = _.clone(idQuery);
	  query['vout.addrs'] =  {$in: s.arr};
	  getTXListFromOutput(s.store, query, requireDetail, 'tx', function(err, txes) {
	    if(err) return c(err);
	    txes.forEach(function(tx) {
	      //txList.push(tx.hash.toString('hex'));
	      txList.push(tx);
	      tx.vout.forEach(function(output, idx) {
		if(output.addrs.length > 0 && sDict[output.addrs[0]]) {
		  keys.push(tx.hash.toString('hex') + '#' + idx);
		}
	      });
	    });
	    c();
	  });
	},
	function(c) {
	  var query = _.clone(idQuery);
	  query['vout.addrs'] =  {$in: s.arr};

	  getTXListFromOutput(s.store, query, requireDetail, 'mempool', function(err, txes) {
	    if(err) return c(err);
	    txes.forEach(function(tx) {
	      txList.push(tx);
	      tx.vout.forEach(function(output, idx) {
		if(output.addrs.length > 0 && sDict[output.addrs[0]]) {
		  keys.push(tx.hash.toString('hex') + '#' + idx);
		}
	      });
	    });
	    c();
	  });
	},
	function(c) {
	  var query = _.clone(idQuery);
	  query['vin.k'] =  {$in: keys};
	  getTXListFromInput(s.store, query, requireDetail, 'tx', function(err, txes) {
	    if(err) return c(err);
	    txes.forEach(function(tx) {
	      txList.push(tx);
	    });
	    c();
	  });
	},
	function(c) {
	  var query = _.clone(idQuery);
	  query['vin.k'] =  {$in: keys};
	  getTXListFromInput(s.store, query, requireDetail, 'mempool', function(err, txes) {
	    if(err) return c(err);
	    txes.forEach(function(tx) {
	      txList.push(tx);
	    });
	    c();
	  });
	},
	function(c) {
	  if(!requireDetail) return c();
	  module.exports.txListToJSON(s.store, txList, function(err, vals) {
	    if(err) return c(err);
	    txList = _.uniq(vals, false, function(tx) {return tx.txid;});
	    c();
	  });
	},
	function(c) {
	  if(requireDetail) return c();
	  txList = _.uniq(txList.map(function(tx){return tx.hash.toString('hex');}));
	  c();
	}
      ], function(err) {
	if(err) return cb(err);
	cb(undefined, {txList: txList, netname:netname});
      });
    };
  };
  var tasks = [];
  for(var netname in storeDict) {
    tasks.push(getTXList(netname));
  }

  async.parallel(tasks, function(err, results) {
    if(err) return callback(err);
    var txIDs = {};
    results.forEach(function(r) {
      if(r.txList.length > 0) {
	txIDs[r.netname] = r.txList;
      }
    });
    callback(undefined, txIDs);
  });
};

module.exports.handleBlock = function(block) {
/*  delete block._id;
  delete block.bits;
  delete block.isMain;
  delete block.nonce;
  block.hash = block.hash.toString('hex');
  if (block.prev_hash) {
    block.prev_hash = block.prev_hash.toString('hex');
  }
  if (block.next_hash) {
    block.next_hash = block.next_hash.toString('hex');
  }
  block.merkle_root = block.merkle_root.toString('hex');
  */
  return {network: block.network, height: block.height};
};
