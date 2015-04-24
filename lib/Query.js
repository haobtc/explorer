var underscore = require('underscore');
var mongodb = require('mongodb');
var async = require('async');
var Defer = require('./Defer');
var bitcore = require('bitcore-multicoin');
var helper = require('./helper');
var blockstore = require('./blockstore');

function segmentAddresses(addressList) {
  var segs = {};
  addressList.forEach(function(addr) {
    addr.possibleNetworks().forEach(function(network) {
      var netname = network.name;
      var s =segs[netname];
      if(s) {
	s.arr.push(addr.toString());
      } else {
	segs[netname] = {
	  netname: netname,
	  arr: [addr.toString()],
	}
      }
    });
  });
  return segs;
};

module.exports.getUnspent = function(addressList, callback) {
  var segs = segmentAddresses(addressList);

  function getUnspent(netname) {
    return function(cb) {
      var s = segs[netname];
      var rpcClient = blockstore[netname];
      rpcClient.getUnspent(s.arr, function(err, arr) {
	if(!err) {
	  arr = arr.map(function(utxo) {return utxo.toJSON()});
	}
	return cb(err, arr);
      });
    };
  };
  var tasks = [];
  for(var netname in segs) {
    tasks.push(getUnspent(netname));
  }
  async.parallel(tasks, function(err, results) {
    if(err) return callback(err);
    var outputs = underscore.flatten(results, true);
    callback(undefined, outputs);
  });
};


module.exports.getTxDetails = function(netname, hashList, callback) {
  var rpcClient = blockstore[netname];
  rpcClient.getTxList(hashList, function(err, arr) {
    if(!err) {
      arr = arr.map(function(tTx) { return tTx.toJSON();});
    }
    return callback(err, arr);
  });
};

// Get TxList via ID
module.exports.getTxListSinceId = function(netname, sinceObjId, colname, callback) {
  var rpcClient = blockstore[netname];
  if(sinceObjId) {
    rpcClient.getTxListSince(sinceObjId, function(err, arr) {
      if(!err) {
	arr = arr.map(function(tTx) { return tTx.toJSON();});
      }
      return callback(err, arr);
    });
  } else {
    rpcClient.getTailTxList(20, function(err, arr) {
      if(!err) {
	arr = arr.map(function(tTx) { return tTx.toJSON();});
      }
      return callback(err, arr);
    });
  }
}


module.exports.getTxListOfAddresses = function(addresses, requireDetail, callback) {
  var segs = segmentAddresses(addresses);
  var results = [];
  function getTXList(netname) {
    return function(cb) {
      var s = segs[netname];
      var rpcClient = blockstore[netname];
      if(requireDetail) {
	rpcClient.getRelatedTxList(s.arr, function(err, arr) {
	  if(!err && arr.length > 0) {
	    arr = arr.map(function(tx){return tx.toJSON();});
	    results.push({netname: netname, txList: arr});
	  }
	  return cb(err, arr);
	});
      } else {
	rpcClient.getRelatedTxIdList(s.arr, function(err,arr) {
	  if(!err && arr.length > 0) {
	    arr = arr.map(function(txId) {
	      return txId.toString('hex');});
	    results.push({netname: netname, txList: arr});
	  }
	  return cb(err, arr);
	});
      }
    };
  };

  var tasks = [];
  for(var netname in segs) {
    tasks.push(getTXList(netname));
  }

  async.parallel(tasks, function(err) {
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

var decodeRawTx = module.exports.decodeRawTx = function(netname, rawtx) {
  var parser = new bitcore.BinaryParser(new Buffer(rawtx, 'hex'));
  var tx = new bitcore.Transaction();
  tx.parse(parser);
  if(tx.serialize().toString('hex') !== rawtx) {
    return c(new helper.UserError('tx_rejected', 'Tx rejected'));
  }
  return tx;
};

module.exports.addRawTx = function(netname, rawtx, info, callback) {
  try {
    var tx = decodeRawTx(netname, rawtx);
    var tTx = new blockstore.ttypes.Tx();
    tTx.netname(netname);
    tTx.fromTxObj(tx);
  } catch(err) {
    console.error(err);
    return callback(err);
  }
  var rpcClient = blockstore[netname];

  async.series([
    function(c) {
      rpcClient.verifyTx(tTx, true, function(err, v) {
	if(err) return c();
	if(!v.verified) return c(new helper.UserError('tx_verify_failed', v.message));
	c();
      });
    },
    function(c) {
      var sendTx = new blockstore.ttypes.SendTx();
      sendTx.hash = tTx.hash;
      sendTx.raw = rawtx;
      if(info.remoteAddress)
	sendTx.remoteAddress = info.remoteAddress;
      rpcClient.sendTx(sendTx, c);
    },
  ], function(err){
    if(err) return callback(err);
    callback(undefined, tTx.hash.toString('hex'));
  });
}
