var async = require('async');
var MongoStore = require('./MongoStore2');

var nodeList = {};

module.exports.stop = function() {
  function stopTask(node) {
    return function(c) {
      node.stop(c);
    };
  };
  var stopTasks = [];
  for(var netname in nodeList) {
    stopTasks.push(stopTask(nodeList[netname]));
  }
  async.parallel(tasks, function(err) {
    if(err) console.error(err);
    // notify master stopped
    process.send({type:'stopped'});
  });
};

module.exports.task = function(msg) {
  var node = nodeList[msg.content.netname];
  node.enqueueBlock(msg.content.blocks);
};

module.exports.start = function(coins) {
  if(typeof coins == 'string') {
    coins = [coins];
  }
  MongoStore.initialize(coins, function(err, netname) {
    if(err) {
      console.error(err);
      return;
    }
    var node = new Node(netname);
    nodeList[netname] = node;
  });
};
