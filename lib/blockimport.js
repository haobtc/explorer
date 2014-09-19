var fs = require('fs');
var bodyParser = require('body-parser');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('./MongoStore');
var Node = require('./Node');
var helper = require('./helper');
var config = require('./config');
var p2pNodes = {};

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  MongoStore.initialize(coins || helper.netnames(), function(err, netname){
    var node = new Node(netname);
    p2pNodes[netname] = node;
    node.updateMempool = true;
    node.synchronize = true;
    //node.syncMethod = 'file';
    node.allowOldBlock = false;
    node.run();
  });
};
