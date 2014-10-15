var async = require('async');
var Node = require('./Node');
var MongoStore = require('./MongoStore2');
var helper = require('./helper');

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
  function format(b) {
    b.hash = new Buffer(b.hash);
    b.merkle_root = new Buffer(b.merkle_root);
    b.prev_hash = new Buffer(b.prev_hash);
    b.txes.forEach(function(tx) {
      tx.hash = new Buffer(tx.hash);
      if(tx.bhash) {
        tx.bhash = new Buffer(tx.bhash);
      }
      tx.vin.forEach(function(input) {
        if(input.hash) input.hash = new Buffer(input.hash);
        input.s = new Buffer(input.s);
      });
      tx.vout.forEach(function(output) {
        output.s = new Buffer(output.s);
      });
    });
  };
  var b = msg.content.block;
  format(b);
  node.enqueueBlock(b);
};

module.exports.start = function(argv) {
  var coins = argv.c;
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
