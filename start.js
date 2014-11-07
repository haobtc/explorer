var argv = require('optimist').argv;
var server;
switch(argv.s) {
case 'node':
  server = require('./bin/nodeserver');
  break;
case 'fetch':
  server = require('./bin/fetchblock');
  break;
default:
  server = require('./bin/queryserver');
  break;
}

var domain = require('domain').create();
domain.on('error', function(err) {
    console.error(err.stack);
});

domain.run(function() {
  server.start(argv);
});

