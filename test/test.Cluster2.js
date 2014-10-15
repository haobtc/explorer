var cluster = require('cluster');

if (cluster.isMaster) {
    console.log('I am master');
      cluster.fork();
        cluster.fork();
} else if (cluster.isWorker) {
    console.log('I am worker #' + cluster.worker.id);
}
