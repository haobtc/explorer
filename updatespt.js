var bitcore = require('bitcore-multicoin');
var Node = require('./lib/Node');
var MongoStore = require('./lib/MongoStore');
var helper = require('./lib/helper');
var config = require('./lib/config');

var p2pNodes = {};

MongoStore.initialize(['dogecoin'], function(err, netname) {
  var node = new Node(netname);
  p2pNodes[netname] = node;
  node.updateMempool = false;
  node.synchronize = false;
  var col = node.store.dbconn.collection('tx');
  node.loopBlocks('jobs.update.spt', 10000, function(block, c) {
    col.find({bhash: block.hash}).toArray(function(err, txes) {
      if(err) return c(err);
      node.updateSpent(txes, c);
    });
  }, function(err) {
    console.info('update spt ok', err);
  });
});

