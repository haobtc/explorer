var cluster = require('cluster');

if(cluster.isMaster) {
  var worker = cluster.fork();
  function stop() {
    worker.send('stop');
    worker.on('message', function(msg) {
      if(msg === 'finish') {
        console.log('receive worker finish');
        worker.kill();
        process.exit();
      }
    });
  }
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
} else {
  setInterval(function() {
    console.info('worker dosomething');
  }, 2000);
  process.on('message', function(msg) {
    if(msg === 'stop') {
      console.log('receive stop');
      setTimeout(function() {
        process.send('finish');
      }, 5000);
    }
  });
}
