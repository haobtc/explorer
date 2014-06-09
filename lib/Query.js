var MongoStore = require('./MongoStore');
var _ = require('underscore');
var async = require('async');
var Defer = require('./Defer');
var bitcore = require('../alliances/bitcore/bitcore');
var helper = require('./helper');

function getStoreDict(addressList) {
  var storeDict = {};
  addressList.forEach(function(addr) {
    var netname = addr.network().name;
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
		address: output.addrs[i],
		network: store.netname,
		vout: vidx,
		scriptPubkey: output.s.toString('hex')
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
    });
    //filterSpent(store, outputs, callback);
    callback(undefined, outputs);
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
	      address: output.addrs[i],
	      network: store.netname,
	      vout: vidx,
	      scriptPubkey: output.s.toString('hex'),
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
	    delete output.key;
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
      var unspent;
      async.series([
	function(c) {
	  getUnspentFromChain(s.store, s.arr, function(err, outputs) {
	    if(err) return c(err);
	    unspent = outputs || [];
	    c();
	  });
	},
	function(c) {
	  getUnspentFromMempool(s.store, s.arr, function(err, outputs, spentInMemPool) {
	    if(err) return c(err);
	    var newUnspent = [];
	    unspent.forEach(function(uspt) {
	      if(!spentInMemPool[unspent.key]) {
		newUnspent.push(uspt);
	      }
	    });
	    outputs.forEach(function(uspt) {
	      if(!spentInMemPool[unspent.key]) {
		newUnspent.push(uspt);
	      }
	    });
	    unspent = newUnspent;
	    unspent.forEach(function(uspt) {
	      delete uspt.key;
	    });
	    c();
	  });
	}], function(err) {
	  if(err) return cb();
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

module.exports.getTxDetails = function(store, hashList, callback) {
  var col = store.dbconn.collection('tx');
  var txList;
  var inputTxDict;
  var blockObjs;
  var txDetails;

  async.series([
    function(c) {
      store.getTxes(hashList, function(err, vals) {
	txList = vals;
	c();
      });
    },
    function(c) {
      var inputTxHashList = [];
      txList.forEach(function(tx) {
	tx.vin.forEach(function(input) {
	  if(input.hash) {
	    inputTxHashList.push(input.hash);
	  }
	});
      });
      inputTxDict = {};
      store.getTxes(inputTxHashList, function(err, vals) {
	if(err) return c(err);
	vals.forEach(function(tx) {
	  inputTxDict[tx.hash.toString('hex')] = tx;
	});
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

	txObj.inputs = tx.vin.map(function(input) {
	  var iObj = {script: input.s.toString('hex')};
	  if(input.hash) {
	    var inputTx = inputTxDict[input.hash.toString('hex')];
	    if(inputTx) {
	      var output = inputTx.vout[input.n];
	      iObj.address = output.addrs.join(',');
	      iObj.amount = helper.satoshiToNumberString(output.v);
	      iObj.hash = input.hash;
	      iObj.vout = input.n;	      
	    }
	  }
	  return iObj;
	});
	txObj.outputs = tx.vout.map(function(output) {
	  return {
	    address: output.addrs.join(','),
	    amount: helper.satoshiToNumberString(output.v)
	  };
	});

	return txObj;
      });
      c();
    }
  ], function(err) {
    if(err && err instanceof Skip) {
      err = null;
    }
    if(err) return callback(err);
    callback(undefined, txDetails);
  });
};



