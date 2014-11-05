var express = require('express');
var bodyParser = require('body-parser');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('./MongoStore');
var NodeSet = require('./NodeSet');
var helper = require('./helper');
var config = require('./config');

var nodeSet = new NodeSet();

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  var sync = true;
  var blockHash;
  if(argv.b) {
    sync = false;
    blockHash = new Buffer(argv.b, 'hex');
    console.info(blockHash.toString('hex') == argv.b);
  }
  nodeSet.run(coins||helper.netnames(), function(node) {
    node.updateMempool = true;
    node.allowOldBlock = false;
    if(sync) {
      node.synchronize = true;
    } else {
      node.synchronize = false;
      node.addBlocks = false;
      node.fetchBlock(blockHash, function(blockObj) {
	console.info('got block', blockObj.txes.length);
	node.updateBlock(blockObj, function(err) {
	  if(err) {
	    console.info(err);
	    throw err;
	  }
	  console.info('updated');
	  process.exit();
	});
      });
    }
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
    if(argv.r) {
      var runsecs = parseInt(argv.r);
      if(!isNaN(runsecs)) {
	setTimeout(stopNode, runsecs * 1000);
      }
    }
  });
};
