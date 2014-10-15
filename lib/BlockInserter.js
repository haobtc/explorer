var async = require('async');
var Node = require('./Node');
var MongoStore = require('./MongoStore2');
var helper = require('./helper');

var nodeList = {};
var blockCacheDict = {};
var blockCacheSize = 0;

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
  async.parallel(stopTasks, function(err) {
    if(err) console.error(err);
    // notify master stopped
    process.send({type:'stopped'});
  });
};

module.exports.task = function(msg) {
  var node = nodeList[msg.content.netname];
  if(node.blockQueue.isFull()) {
    console.info('block is full');
    return;
  }
  //save memory
  if(blockCacheSize >= 10000) {
    blockCacheSize = 0;
    blockCacheDict = {};
  }
  // filter duplicated blocks
  var b = msg.content.block;
  b.hash = new Buffer(b.hash);
  if(blockCacheDict[b.hash.toString('hex')]) {
    return;
  }
  blockCacheDict[b.hash.toString('hex')] = true;
  blockCacheSize++;

  function format(b) {
    //b.hash = new Buffer(b.hash);
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
    // skipped blocks should be re-enqueue
    node.on('block skip', function(hash) {
      delete blockCacheDict[hash];
    });
    nodeList[netname] = node;
    setInterval(function() {
      console.info('blockQueue length=%d', node.blockQueue.size());
    }, 4000);
  });
};
