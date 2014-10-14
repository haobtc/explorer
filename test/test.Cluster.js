var cluster = require('cluster');

if(cluster.isMaster) {
  var worker = cluster.fork();
  function stop() {
  };
} else if(cluster.isWorker) {
  var net = require('net');
  var server = net.createServer(function(socket) {
  });
  server.listen(8337);

  process.on('message', function(msg) {
    if(msg === 'shutdown') {
      console.info('shutdown');
    }
  });
}
