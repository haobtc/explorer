var os = require('os');
var underscore = require('underscore');
var express = require('express');
var cluster = require('cluster');
var NodeSet = require('../lib/NodeSet');
var helper = require('../lib/helper');
var config = require('../lib/config');

var nodeSet = new NodeSet();

function startNode(netname, argv) {
  var runsecs;
  if(argv.r) {
    runsecs = parseInt(argv.r);
    if(!isNaN(runsecs)) {
      nodeSet.stopTime = new Date().getTime() + runsecs * 1000;
    }
  }
  nodeSet.run([netname], function(node) {
    node.start(argv);
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

    if(!isNaN(runsecs)) {
      setTimeout(stopNode, runsecs * 1000 + Math.random() * 2000);
    }
  });
}

var workerList = [];
module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }

  var tmp = [];
  var coinNProcesses = {};
  coins.forEach(function(netname) {
    var idx = netname.indexOf(':');
    var n = 1;
    if(idx >= 0) {
      n = parseInt(netname.substr(idx+1));
      n = Math.max(Math.min(n, os.cpus().length), 1);
      netname = netname.substr(0, idx);
    }
    coinNProcesses[netname] = n;
    tmp.push(netname);
  });
  coins = tmp;

  coins = coins || helper.netnames();

  if(cluster.isMaster) {
    function forkWorker(env) {
      var worker = cluster.fork(env);
      workerList.push({env: env, worker: worker});
    }

    coins.forEach(function(netname) {
      var n = coinNProcesses[netname];
      for(var i=0; i<n; i++) {
	forkWorker({'NETNAME': netname});
      }
    });

    cluster.on('exit', function(worker, code, signal) {
      for(var i=0; i<workerList.length; i++) {
	var w = workerList[i];
	if(w.worker.id == worker.id) {
	  console.info('worker exit', w.env, code, signal, worker.suicide);
	  if(!worker.suicide) {
	    w.worker = cluster.fork(w.env);
	  }
	  break;
	}
      }
    });
  } else {
    var netname = process.env.NETNAME;
    startNode(netname, argv);
  }
};

