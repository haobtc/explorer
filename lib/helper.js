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

function CallFIFO() {
  this.fifo = [];
  this.calling = false;
}

CallFIFO.prototype.unshift = function(fn, args, callback) {
  var self = this;
  this.fifo.unshift({
    fn: fn,
    args: args,
    callback: callback,
      insertDate: new Date()
  });
  function pickup() {
    if(self.calling) {
      return;
    }
    var entry = self.fifo.pop();
    if(entry) {
      self.calling = true;
      entry.args.push(function() {
	var results = [];
	for(var i=0; i< arguments.length; i++) {
	  results.push(arguments[i]);
	}
	if(typeof entry.callback == 'function') {
/*	    console.info('task takes', 
			 (entry.startDate - entry.insertDate)/1000, 'seconds and',
			 (new Date() - entry.startDate)/1000, 'seconds to exe'); */
	  entry.callback.apply(this, results);
	}
	self.calling = false;
	setTimeout(pickup, 0);
      });
      entry.startDate = new Date();
      entry.fn.apply(undefined, entry.args);
    }
  }
  pickup();
};

CallFIFO.prototype.isCalling = function() {
  //return this.calling || this.fifo.length > 0;
  return this.fifo.length > 0;
}

module.exports.CallFIFO = CallFIFO;

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
