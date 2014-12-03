var underscore = require('underscore');
var express = require('express');
var cluster = require('cluster');
var NodeSet = require('../lib/NodeSet');
var helper = require('../lib/helper');
var config = require('../lib/config');
var chainserver = require('./chainserver');

var nodeSet = new NodeSet();

function startNode(netname, argv) {
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

    if(argv.r) {
      var runsecs = parseInt(argv.r);
      if(!isNaN(runsecs)) {
	setTimeout(stopNode, runsecs * 1000);
      }
    }
  });
}

var workerList = [];
module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  coins = coins || helper.netnames();

  var startPort = parseInt(argv.p);

  if(cluster.isMaster) {
    function forkWorker(env) {
      var worker = cluster.fork(env);
      workerList.push({env: env, worker: worker});
    }

    coins.forEach(function(netname) {
      if(config.networks[netname].remoteBlockChain.enabled) {
	forkWorker({'PARG': netname,
	 'REMOTE_CHAIN_URL': 'http://localhost:' + startPort});
	forkWorker({'PARG': 'p2p',
		    'PORT': startPort});
	startPort++;
      } else {
	forkWorker({'PARG': netname});
      }
    });

    cluster.on('exit', function(worker, code, signal) {
      for(var i=0; i<workerList.length; i++) {
	var w = workerList[i];
	if(w.worker.id == worker.id) {
	  console.info('worker exit', w.env, code, signal, worker.suicide);
	  //w.died = true;
	  if(!worker.suicide) {
	    console.info('suicide');
	    w.worker = cluster.fork(w.env);
	  }
	  break;
	}
      }
      var aliveWorkerList = underscore.reject(workerList, function(w) {
	return w.died;
      });
      console.info('alives', aliveWorkerList.length);
    });
  } else {
    if(process.env.PARG == 'p2p') {
      chainserver.start({p: process.env.PORT, r: argv.r});
    } else {
      startNode(process.env.PARG, argv);
    }
  }
};
