var express = require('express');
var cluster = require('cluster');
var NodeSet = require('../lib/NodeSet');
var helper = require('../lib/helper');
var config = require('../lib/config');
var chainserver = require('./chainserver');

var nodeSet = new NodeSet();

function startNode(netname) {
  nodeSet.run([netname], function(node) {
    node.updateMempool = true;
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
  });
}

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  coins = coins || helper.netnames();

  var startPort = parseInt(argv.p);

  if(cluster.isMaster) {
    coins.forEach(function(netname) {
      if(config.networks[netname].remoteBlockChain.enabled) {
	cluster.fork({'PARG': netname,
		      'REMOTE_CHAIN_URL': 'http://localhost:' + startPort});
	cluster.fork({'PARG': 'p2p',
		      'PORT': startPort});
	startPort++;
      } else {
	cluster.fork({'PARG': netname});
      }
    });
  } else {
    if(process.env.PARG == 'p2p') {
      chainserver.start({p: process.env.PORT});
    } else {
      startNode(process.env.PARG);
    }
  }
};
