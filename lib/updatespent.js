var MongoStore = require('./MongoStore2');
var helper = require('./helper');
var async = require('async');
var Node = require('./Node');

function UpdateSpent(netname) {
  var self = this;
  this.netname = netname;
  this.node = new Node(netname);
}

UpdateSpent.prototype.run = function(cb) {
  var self = this;
  this.updateSpentTimer = setInterval(function() {
    if(self.node.processingSpent) return;
    self.node.processingSpent = true;
    self.node.processSpentQueue.task({timestamp:new Date(), cursor:self.node.processingSpentCursor},
                                     function(c) {
                                       self.node._processSpent(function(err, finish) {
                                         if(err) console.error(err);
                                         self.node.processingSpent = false;
                                         c();
                                       });
                                     });
  }, 1000);

  this.removeObsoleteTimer = setInterval(function() {
    if(self.node.removingObsolete) return;
    self.node.removingObsolete = true;
    self.node.processSpentQueue.task({timestamp:new Date(), cursor:'remove obsolete'},
                                function(c) {
                                  self.node.handleObsoleteTxList(function(err) {
                                    if(err) console.error(err);
                                    self.node.removingObsolete = false;
                                    c();
                                  });
                                });

  }, 30*1000);
};

UpdateSpent.prototype.stop = function(cb) {
  var self = this;
  console.info('updatespent '+self.netname+' stopping');
  self.node.processSpentQueue.task({timestamp:new Date(), cursor:''},
                              function(c) {
                                self.node.processSpentQueue.closed = true;
                                console.info('updatespent '+self.netname+' stopped');
                                cb();
                                c();
                              }, {use:'close process spent queue'});
};

module.exports.start = function(argv) {
  var coins = argv.c;
  if(typeof coins == 'string') coins = [coins];
  MongoStore.initialize(coins || helper.netnames(), function(err, netname) {
    if(err) console.error(err);
    var updateSpentDict = {};
    for(var netname in MongoStore.stores) {
      var store = MongoStore.stores[netname];
      var updateSpent = new UpdateSpent(netname);
      updateSpentDict[netname] = updateSpent;
      updateSpent.run();
    }

    function stopUpdateSpent() {
      console.info('stop update spent');
      function stopTask(entry) {
        return function(c) {
          entry.stop(c);
        }
      }
      var tasks = [];
      for(var netname in updateSpentDict) {
        tasks.push(stopTask(updateSpentDict[netname]));
      }
      async.parallel(tasks, function(err) {
        if(err) console.error(err);
        process.exit();
      });
    }
    process.on('SIGINT',stopUpdateSpent);
    process.on('SIGTERM',stopUpdateSpent);
  });
}
