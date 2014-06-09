var argv = require('optimist').argv;
var server = argv.s == 'node'?require('./lib/nodeserver'):require('./lib/queryserver');

var domain = require('domain').create();
domain.on('error', function(err) {
    console.error(err.stack);
});

domain.run(function() {
  server.start(argv);
});

