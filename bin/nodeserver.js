var express = require('express');
var bodyParser = require('body-parser');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('../lib/MongoStore');
var NodeSet = require('../lib/NodeSet');
var helper = require('../lib/helper');
var config = require('../lib/config');

var nodeSet = new NodeSet();

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  var endDate = null;
  if(argv.r) {
    var runsecs = parseInt(argv.r);
    if(!isNaN(runsecs)) {
      endDate = new Date().getTime() + runsecs * 1000;
    }
  }

  console.info(endDate);
  nodeSet.run(coins||helper.netnames(), function(node) {
    node.endDate = endDate;
    node.peerLimit = 250;
    if(argv.denyMempool) {
      node.updateMempool = false;
    } else {
      node.updateMempool = true;
    }
    node.allowOldBlock = false;
    node.synchronize = true;
  }, function(err) {
    if(err) throw err;
    function stopNode() {
      console.log('stopping node server\n');
      nodeSet.stop(function(){
	console.log('stopped\n');
	process.exit();
      });
    }
    process.on('SIGINT', stopNode);
    process.on('SIGTERM', stopNode);
    if(endDate) {
      setTimeout(stopNode, endDate - (new Date().getTime()));
    }
  });
};
