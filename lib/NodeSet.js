var async = require('async');
//var MongoStore = require('./MongoStore');
var Node = require('./Node');

function NodeSet() {
  this.nodes = {};
}

NodeSet.prototype.node = function(netname) {
  return this.nodes[netname];
};

NodeSet.prototype.add = function(node) {
  // TODO: check the existance of nodes[netname]
  this.nodes[node.netname] = node;
  node.nodeSet = this;
};

NodeSet.prototype.stop = function(cb) {
  function stopTask(node) {
    return function(c) {
      node.stop(c);
    };
  }
  var tasks = [];
  for(var netname in this.nodes) {
    tasks.push(stopTask(this.nodes[netname]));
  }
  async.parallel(tasks, cb);
};

NodeSet.prototype.run = function(coins, eachNode, complete) {
  var self = this;
  //MongoStore.initialize(coins, function(err, netname) {
  coins.forEach(function(netname) {
    var node = new Node(netname);
    self.add(node);
    eachNode(node);
    node.run();    
  });
  complete();
  //}, complete);
};

module.exports = NodeSet;
