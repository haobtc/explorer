
function Queue(opts) {
  opts = opts||{};
  this.fifo = [];
  this.timeout = opts.timeout;
  this.limit = opts.limit;
  this.counter = 1;
  this.cTask = null;
}

Queue.prototype.task = function(fn, info) {
  if(this.limit != undefined &&
     this.fifo.length >= this.limit) {

    if(this.cTask) {
      console.warn('task queue is full', this.cTask.info);
    } else {
      console.warn('task queue is full');
    }
    return;
  }

  this.fifo.unshift({
    counter: this.counter++,
    info: info,
    fn: fn,
    queueTime: new Date()
  });
  this.pickup();
};

Queue.prototype.pickup = function() {
  var self = this;
  if(this.cTask) {
    return;
  }
  var task = self.fifo.pop();
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
    task.fn(function(err) {
      if(err) throw err;
      if(self.cTask && self.cTask.counter == task.counter) {
	self.cTask = null;
	if(Math.random() < 0.05) {
	  setTimeout(self.pickup.bind(self), 0);
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
  return this.fifo.length + (!!this.cTask?1:0);
};

Queue.prototype.isFull = function() {
  if(this.limit != undefined) {
    return this.size() >= this.limit;
  } else {
    return false;
  }
};

module.exports = Queue;
