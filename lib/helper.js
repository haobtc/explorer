var buffertools    = require('buffertools');
var bitcore = require('../alliances/bitcore/bitcore');
var mongodb = require('mongodb');
var config = require('./config');
var Script = bitcore.Script;
var util = bitcore.util;

module.exports.reverseBuffer = function(hash) {
  var reversed = new Buffer(hash.length);
  hash.copy(reversed);
  buffertools.reverse(reversed);
  return reversed;
}

module.exports.toBuffer = function(hash) {
  if(hash instanceof mongodb.Binary) {
    return hash.buffer;
  } else if(hash instanceof Buffer) {
    return hash;
  } else {
    return new Buffer(hash, 'hex');
  }
};

module.exports.clone = function(src) {
  var dest = new Object();
  for(var key in src) {
    dest[key] = src[key];
  }
  return dest;
}

module.exports.satoshiToNumberString = function(v) {
  if(!v || v == '0') {
    return '0'
  }
  while(v.length < 8) {
    v = '0' + v;
  }
  v = v.replace(/\d{8}$/, '.$&').replace(/0+$/, '').replace(/\.$/, '.0');
  if(v.substr(0, 1) == '.') {
    v = '0' + v;
  }
  return v;
};

module.exports.netnames = function() {
  var netnames = [];
  for(var netname in config.networks) {
    netnames.push(netname);
  }
  return netnames;
};

module.exports.processTx = function(netname, tx, idx, blockObj) {
  var txObj = {};
  txObj.hash = module.exports.reverseBuffer(tx.hash);
  if(idx >= 0) {
    if(!blockObj) {
      console.error('processTx:blockObj null:netname=%s:txHash=%s:idx=%d',
          netname, txObj.hash.toString('hex'), idx);
    }
    txObj.bhash = blockObj.hash;
    txObj.bidx = idx;
  }
  if(tx.version != 1) {
    txObj.v = tx.version;
  }
  if(tx.lock_time != 0) {
    txObj.lock_time = tx.lock_time;
  }
  txObj.vin = tx.ins.map(function(input, i) {
    var txIn = {};
    var n = input.getOutpointIndex();
    if(n >= 0) {
      txIn.hash = module.exports.reverseBuffer(new Buffer(input.getOutpointHash()));
      txIn.n = n;
      
      txIn.k = txIn.hash.toString('hex') + '#' + n;
    }
    txIn.s = input.s;
    txIn.q = input.q;
    return txIn;
  });

  txObj.vout = tx.outs.map(function(out, i) {
    var txOut = {};
    txOut.s = out.s;
    if(tx.outs[i].s) {
      var script = new Script(tx.outs[i].s);
      txOut.addrs = script.getAddrStr(netname);
    }
    txOut.v = util.valueToBigInt(out.v).toString();
    txOut.spent = 0;
    return txOut;
  });
  return txObj;
};

function Skip(message) {
  this.message = message;
}

module.exports.Skip = Skip;
