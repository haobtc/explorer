var argv = require('optimist').argv;
var server = require('./lib/server');

var domain = require('domain').create();
domain.on('error', function(err) {
    console.error(err.stack);
});

domain.run(function() {
    server.httpServer.listen(argv.p || 9000);
});

