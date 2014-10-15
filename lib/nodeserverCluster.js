var cluster = require('cluster');

module.exports.start = function(argv) {
  if(cluster.isMaster) {
    var nodeserverWorker = cluster.fork();
    var blockImporter = cluster.fork();
    var onlineCnt = 0;
    cluster.on('online', function(worker) {
      if(worker.id == 1) {
        console.info('nodeserver online');
      }
      if(worker.id == 2) {
        console.info('blockimporter online');
      }
    });
   //stop related
    var stoppedCount = 0;
    function workerMessageHandler(msg) {
      switch(msg.type) {
      case 'stopped':
        if(++stoppedCount >= 2) {
          nodeserverWorker.kill();
          blockImporter.kill();
          process.exit();
        }
        break;
      case 'block':
        blockImporter.send(msg);
        break;
      default:
        console.error('master unknown msg type:type=%s', msg.type);
        break;
      }
    };
    Object.keys(cluster.workers).forEach(function(id) {
      cluster.workers[id].on('message', workerMessageHandler);
    });
    function stop() {
      cluster.workers.forEach(function(worker) {
        worker.send({type:'stop'});
      });
    };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);
  } else {
    // nodeserver
    var curWorkerId = cluster.worker.id;
    if(curWorkerId == 1) {
      var server = require('./nodeserver');
      server.on('block', function(val) {
        //console.log('nodeserverCluster:bhash=%s', val.block.hash.toString('hex'));
        process.send({type:'block', content:{netname:val.netname, block:val.block}});
      });
      server.start(argv);
      process.on('message', function(msg) {
        switch(msg.type) {
          case 'stop':
            server.stop();
            break;
          default:
            console.error('nodeserver unknown msg type:type=%s', msg.type);
          break;
        }
      });
    }
    // blockinserter
    if(curWorkerId == 2) {
      var server = require('./BlockInserter');
      server.start(argv);
      process.on('message', function(msg) {
        switch(msg.type) {
          case 'stop':
            server.stop();
            break;
          case 'block':
            server.task(msg);
            break;
          default:
            console.error('blockinserter unknown msg type:type=%s', msg.type);
          break;
        }
      });
    }
 }
};
