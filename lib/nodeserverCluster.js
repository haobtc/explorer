var cluster = require('cluster');

module.exports.start = function(argv) {
  if(cluster.isMaster) {
    var nodeserverWorker = cluster.fork();
    var blockImporter = cluster.fork();
    cluster.on('fork', function(worker) {
      if(worker.id == nodeserverWorker.id) {
        nodeserverWorker.send({type:'nodeserver'});
      } else if(worker.id == blockImporter.id) {
        blockImporter.send({type:'blockinserter'});
      }
    });
    //stop related
    var stoppedCount = 0;
    function workerMessageHandler(msg) {
      if(msg.type === 'stopped') {
        if(++stoppedCount >= 2) {
          process.exit();
        }
      }
    };
    Object.keys(cluster.workers).forEach(function(id) {
      cluster.workers[id].on('message', workerMessageHandler);
    });
    function stop() {
      cluster.workers.ForEach(function(worker) {
        worker.send({type:'stop'});
      });
    };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);
  } else {
    var server = null;
    var serverType = '';
    process.on('message', function(msg) {
      switch(msg.type) {
        case 'nodeserver':
          server = require('./nodeserver');
          serverType = 'nodeserver';
          server.on('block', function(b) {
            var worker = cluster.workers[1];
            worker.send({type:'block', {netname:node.netname, block:b}});
          });
          server.start(argv);
          break;
        case 'blockinserter':
          server = require('./BlockInserter');
          serverType = 'blockinserter';
          server.start(argv);
          break;
        case 'block':
          if(serverType !== 'blockinserter') {
            console.error('server type wrong:type=%s', serverType);
          }
          var worker = cluster.workers[2];
          worker.send(msg);
          break;
        case 'stop':
          server.stop();
          break;
        default:
          console.error('unknown msg:type=%s', msg.type);
          break;
      }
    });
  }
};
