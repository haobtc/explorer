var JumpList = require('jumplist');

function Queue(opts) {
  opts = opts||{};
  this.fifo = new JumpList(function(k1, k2) {
    if(k1.timestamp > k2.timestamp) return 1;
    else if (k1.timestamp < k2.timestamp) return -1;
    else if (k2.hexHash > k2.hexHash) return 1;
    else if (k2.hexHash < k2.hexHash) return -1;
    else return 0;
  });
  this.timeout = opts.timeout;
  this.limit = opts.limit;
  this.counter = 1;
  this.cTask = null;
  this.closed = false;
}

Queue.prototype.task = function(key, fn, info) {
  if(this.closed) {
    return false;
  }

  if(this.limit != undefined &&
     this.fifo.length >= this.limit) {

    if(this.cTask) {
      console.warn('task queue is full', this.cTask.info);
    } else {
      console.warn('task queue is full');
    }
    return false;
  }

  this.fifo.set(key, {
    counter: this.counter++,
    info: info,
    fn: fn,
    queueTime: new Date()
  });
  this.pickup();
  return true;
};

Queue.prototype.pickup = function() {
  var self = this;
  if(this.cTask) {
    return;
  }
  var task;
  var item = self.fifo.getAt(0);
  if(item) {
    self.fifo.remove(item.key);
    task = item.value;
  }
  if(task) {
    this.cTask = task;
    if(this.timeout != undefined) {
      setTimeout(function() {
	if(self.cTask && self.cTask.counter == task.counter) {
	  console.warn('task', task.counter, 'timeout, info=', task.info);
	  self.cTask = null;
	  setTimeout(self.pickup.bind(self), 0);
	}
      }, this.timeout);
    }
    this.cTask.startTime = new Date();
    var fn = task.fn;
    if(this.closed) {
      fn = function(c) {return c();}
    }
    fn(function(err) {
      if(err) throw err;
      if(self.cTask && self.cTask.counter == task.counter) {
	self.cTask = null;
	if(Math.random() < 0.05) {
	  //setTimeout(self.pickup.bind(self), 0);
	  process.nextTick(self.pickup.bind(self));
	} else {
	  self.pickup();
	}
      } else {
	console.warn('timeout task', task.counter, 'returned in', (new Date() - task.startTime)/1000, 'seconds, info=', task.info);
      }
    });
  }
};

Queue.prototype.size = function() {
  var fsz = this.fifo.size();
  return fsz + (!!this.cTask?1:0);
};

Queue.prototype.isFull = function() {
  if(this.limit != undefined) {
    return this.size() >= this.limit;
  } else {
    return false;
  }
};

Queue.prototype.isAlmostFull = function() {
  if(this.limit != undefined) {
    return this.size() >= this.limit * 0.8;
  } else {
    return false;
  }
};

module.exports = Queue;
