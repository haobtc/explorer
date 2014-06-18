var buffertools    = require('buffertools');
var mongodb = require('mongodb');
var config = require('./config');

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
  if(v == '0') {
    return 0;
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
}

function Skip(message) {
  this.message = message;
}

module.exports.Skip = Skip;
