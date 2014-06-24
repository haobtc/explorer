var argv = require('optimist').argv;
var server;
switch(argv.s) {
case 'node':
  server = require('./lib/nodeserver');
  break;
case 'import':
  server = require('./lib/blockimport');
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

