var buffertools    = require('buffertools');

module.exports.reverseBuffer = function(hash) {
  var reversed = new Buffer(hash.length);
  hash.copy(reversed);
  buffertools.reverse(reversed);
  return reversed;
}

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
  return this.calling || this.fifo.length > 0;
}

module.exports.CallFIFO = CallFIFO;
