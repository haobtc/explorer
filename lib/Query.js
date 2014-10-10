var MongoStore = require('./MongoStore2');
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
    if(!tx.binfo) return;
    tx.binfo.forEach(function(e) {
      blockHashes.push(e.bhash);
    });
  });

  var col = store.dbConn.collection('block');
  col.find({hash: {$in: blockHashes}}).toArray(function(err, blocks) {
    if(err) return callback(err, undefined);
    var blockObjs = {};
    blocks.forEach(function(block) {
      blockObjs[block.hash.toString('hex')] = block;
    });
    callback(undefined, blockObjs);
  });
}

function filterTxList(txList, blockObjs) {
  for(var i = txList.length - 1; i >= 0; --i) {
    var tx = txList[i];
    if(tx.binfo) {
      var filter = true;
      for(var j = 0; j < tx.binfo.length; ++j) {
        var e = tx.binfo[j];
        var b = blockObjs[e.bhash.toString('hex')];
        if(b.isMain) {
          filter = false;
          break;
        }
      }
      if(filter) txList.splice(i, 1);
    }
  }
  return txList;
}

module.exports.getUnspent = function(addressList, callback) {
  var storeDict = getStoreDict(addressList);
  function getUnspent(netname) {
    return function(cb) {
      var s = storeDict[netname];
      var unspent = [];
      var txList = [];
      var blockObjs = [];
      var addrDict = {};
      s.arr.forEach(function(addr) {
        addrDict[addr] = true;
      });
      async.series([
        function(c) {
          var txCol = s.store.dbConn.collection('tx');
          txCol.find({'vout.addrs' : {$in:s.arr}, 'vout.spent':{$size:0}}).toArray(function(err, txes) {
            if(err) return c(err);
            txList = txes;
            c();
          });
        },
        function(c) {
          getBlocks(s.store, txList, function(err, blocks) {
            if(err) return c(err);
            blockObjs = blocks;
            filterTxList(txList, blockObjs);
            c();
          });
        },
        function(c) {
          txList.forEach(function(tx) {
            var block;
            if(tx.binfo) {
              for(var i = 0; i < tx.binfo.length; ++i) {
                var e = tx.binfo[i];
                var b = blockObjs[e.bhash.toString('hex')];
                if(b.isMain) {
                  block = b;
                  break;
                }
              }
            }
            tx.vout.forEach(function(output, idx) {
              for(var i = 0; i < output.addrs.length; ++i) {
                if(addrDict[output.addrs[i]]) {
                  var obj = {
                    txid: tx.hash.toString('hex'),
                    amount: helper.satoshiToNumberString(output.v),
                    amountSatoshi: output.v.toString(),
                    address: output.addrs[i],
                    network: s.store.netname,
                    vout: idx,
                    scriptPubKey: output.s.toString('hex')
                  };
                  if(block) {
                    obj.confirmations = Math.max(0, s.store.max_height - block.height + 1);
                    obj.time = block.timestamp;
                  } else {
                    obj.confirmations = 0;
                    obj.time = Math.round(new Date().getTime()/1000);
                  }
                  unspent.push(obj);
                }
              }
            });
          });
          c();
        },
      ],function(err) {
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
        filterTxList(txList, blockObjs);
	c();
      });
    },
    function(c) {
      txDetails = txList.map(function(tx, vindex) {
	var block;
        if(tx.binfo) {
          for(var i = 0; i < tx.binfo.length; ++i) {
            var e = tx.binfo[i];
            var b = blockObjs[e.bhash.toString('hex')];
            if(b.isMain) {
              block = b;
              break;
            }
          }
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
		var txcol = store.dbConn.collection('tx');

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
	    spent: output.spent
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
  store.getTxList(inputTxHashList, function(err, vals) {
    if(err) return callback(err);
    vals.forEach(function(tx) {
      inputTxDict[tx.hash.toString('hex')] = tx;
    });
    callback(undefined, inputTxDict);
  });
};

module.exports.getTxDetails = function(store, hashList, callback) {
  var txList;
  var txDetails;

  async.series([
    function(c) {
      store.getTxList(hashList, function(err, vals) {
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
  var col = store.dbConn.collection(colname);
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
  var col = store.dbConn.collection(colname);
  if(requireDetail) {
    col.find(query).toArray(callback);
  } else {
    col.find(query, ['hash', 'vout']).toArray(callback);
  }
};

function getTXListFromInput(store, query, requireDetail, colname, callback) {
  var col = store.dbConn.collection(colname);
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
