var bitcore = require('./alliances/bitcore/bitcore');
var Node = require('./lib/Node');
var MongoStore = require('./lib/MongoStore');
var helper = require('./lib/helper');
var config = require('./lib/config');

var p2pNodes = {};

MongoStore.initialize(['bitcoin', 'litecoin', 'dogecoin'], function(err, netname) {
  var node = new Node(netname);
  p2pNodes[netname] = node;
  node.updateMempool = false;
  node.run();
});

