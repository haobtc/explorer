var cluster = require('cluster');

if(cluster.isMaster) {
  var worker1 = cluster.fork();
  var worker2 = cluster.fork();
  cluster.on('fork', function(worker) {
    console.log('onFork:workerId=%d', worker.id);
    if(worker.id == worker1.id) worker1.send('collector');
    if(worker.id == worker2.id) worker2.send('importer');
  });
} else {
  process.on('message', function(msg) {
    console.log(msg);
  });
}
