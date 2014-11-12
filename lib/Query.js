var MongoStore = require('./MongoStore');
var underscore = require('underscore');
var mongodb = require('mongodb');
var async = require('async');
var Defer = require('./Defer');
var bitcore = require('bitcore-multicoin');
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
    store.getBlocks(txes, function(err) {
      if(err) return callback(err);
      txes.forEach(function(tx) {
	var block = getTxBlock(tx);
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
		obj.confirmations = Math.max(0, store.stats.maxHeight - block.height + 1);
		obj.time = block.timestamp;
	      } else {
		obj.confirmations = 0;
		obj.time = Math.round(new Date().getTime()/1000);
	      }
	      //obj.key = obj.txid + '#' + obj.vout;
	      outputs.push(obj);
	      break;
	    }
	  }
	});
      });
      callback(undefined, outputs);
    }); // end of getBlocks
  });
}

module.exports.getUnspent = function(addressList, callback) {
  var storeDict = getStoreDict(addressList);

  function getUnspent(netname) {
    return function(cb) {
      var s = storeDict[netname];
      getUnspentFromChain(s.store, s.arr, cb);
    };
  };
  var tasks = [];
  for(var netname in storeDict) {
    tasks.push(getUnspent(netname));
  }
  async.parallel(tasks, function(err, results) {
    if(err) return callback(err);
    var outputs = underscore.flatten(results, true);
    callback(undefined, outputs);
  });
};

function getTxBlock(tx) {
  if(!tx.blocks) {
    return null;
  }
  var maxHeight = -1;
  var selected;
  tx.blocks.forEach(function(block, i) {
    if(!block || !block.isMain) {
      return;
    }
    if(block.height > maxHeight) {
      selected = block;
      maxHeight = block.height;
      tx.bidx = tx.bis[i];
    }
  });
  return selected;
}

module.exports.txListToJSON = function(store, txList, callback) {
  var txDetails;
  var inputTxDict = {};
  async.series([
    function(c) {
      store.getBlocks(txList, c);
    },
    function(c) {
      inputTxes(store, txList, function(err, vals) {
	if(err) return c(err);
	inputTxDict = vals;
	c();
      });
    },
    function(c) {
      txDetails = txList.map(function(tx, vindex) {
	var block = getTxBlock(tx);
        var txObj = {
	  ts: tx._id.getTimestamp().getTime(),
	  id: tx._id.toString(),
          network: store.netname,
          txid: tx.hash.toString('hex'),
	  inputs: [],
	  outputs: [],
        };
	if(tx.bidx != undefined) {
	  txObj.blockindex = tx.bidx;
	}
	if(block) {
          txObj.blockhash = block.hash.toString('hex');
          txObj.blocktime = block.timestamp;
	  txObj.blockheight = block.height;
          txObj.time = block.timestamp;
	  txObj.confirmations = Math.max(0, store.maxHeight - block.height + 1);
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
	  var query = underscore.clone(idQuery);
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
	  var query = underscore.clone(idQuery);
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
	  if(!requireDetail) return c();
	  module.exports.txListToJSON(s.store, txList, function(err, vals) {
	    if(err) return c(err);
	    txList = underscore.uniq(vals, false, function(tx) {return tx.txid;});
	    c();
	  });
	},
	function(c) {
	  if(requireDetail) return c();
	  txList = underscore.uniq(txList.map(function(tx){return tx.hash.toString('hex');}));
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

module.exports.addRawTx = function(netname, rawtx, info, callback) {
  try {
    var parser = new bitcore.BinaryParser(new Buffer(rawtx, 'hex'));
  } catch(err) {
    console.error(err);
    return callback(err);
  }

  var store = MongoStore.stores[netname];
  var txObj;

  async.series([
    function(c) {
      var tx = new bitcore.Transaction();
      tx.parse(parser);
      if(tx.serialize().toString('hex') !== rawtx) {
	return c(new helper.UserError('tx_rejected', 'Tx rejected'));
      }
      txObj = helper.processTx(netname, tx);
      c();
    },
    function(c) {
      store.getTx(txObj.hash, function(err, obj) {
	if(err) return c();
	if(obj) return c(new helper.UserError('tx_exist', 'Transaction already in block chain'));
	c();
      });      
    },
    function(c) {
      store.verifyTx(txObj, function(err, verified) {
	if(err) return c();
	if(!verified) return c(new helper.UserError('tx_verify_failed', 'Tx verify failed'));
	c();
      });
    },
    function(c) {
      var col = store.dbconn.collection('sendtx');
      var sendtx = {
	hash: txObj.hash,
	raw: new Buffer(rawtx, 'hex'),
	sent: false,
	info: info
      };
      col.findAndModify(
	{'hash': txObj.hash}, [],
	{$set: sendtx},
	{upsert: true},
	function(err, obj) {
	  if(err) return c(err);
	  c();
	});
    },
    function(c) {
      store.addTxes([txObj], c);
    },
  ], function(err){
    if(err) return callback(err);
    callback(undefined, txObj.hash.toString('hex'));
  });
}
