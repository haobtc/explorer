var MongoStore = require('./MongoStore');
var Defer = require('./Defer');
var bitcore = require('../alliances/bitcore/bitcore');
var helper = require('./helper');

function getStoreDict(addressList) {
  var storeDict = {};
  addressList.forEach(function(address) {
    var lead = address.substr(0, 1);
    var addr = new bitcore.Address(address);
    var netname = addr.network().name;
    
    var s = storeDict[netname];
    if(s) {
      s.arr.push(address);
    } else {
      storeDict[netname] = {
	netname: netname,
	arr: [address],
	store: MongoStore.stores[netname]
      }
    }
  });
  return storeDict;
};

function getUnspentForStore(store, addressList, callback) {
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
    txes.forEach(function(tx) {
      tx.vout.forEach(function(output, vidx) {
	for(var i=0; i<output.addrs.length; i++) {
	  if(!!addrDict[output.addrs[i]]) {
	    var obj = {
	      txid: tx.hash.toString('hex'),
	      amount: helper.satoshiToNumberString(output.v),
	      address: output.addrs[i],
	      network: store.netname,
	      vout: vidx,
	      scriptPubkey: output.s.toString('hex')
	    }
	    obj.key = obj.txid + '#' + obj.vout;
	    outputs.push(obj);
	    break;
	  }
	}
      });
    });
    filterSpent(store, outputs, callback);
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
    var s = storeDict[netname];
    var d = Defer();
    getUnspentForStore(s.store, s.arr, function(err, outputs) {
      if(err) throw err;
      outputs = outputs || [];
      d.avail.apply(null, outputs);
    });
    return d;
  }
  var childDefers = [];
  for(var netname in storeDict) {
    var d = getUnspent(netname);
    childDefers.push(d);
  }
  var defer = Defer();
  defer.wait(childDefers, {flatten: true});
  defer.then(function(outputs) {
    //console.info('outputs', outputs);
    callback(outputs);
  });
};


