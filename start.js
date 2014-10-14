var argv = require('optimist').argv;
var server;
switch(argv.s) {
case 'node':
  server = require('./lib/nodeserverCluster');
  break;
case 'import':
  server = require('./lib/blockimport');
  break;
case 'update':
  server = require('./lib/updatespent');
  break;
default:
  server = require('./lib/queryserver');
  break;
}

var domain = require('domain').create();
domain.on('error', function(err) {
    console.error(err.stack);
});

domain.run(function() {
  server.start(argv);
});

