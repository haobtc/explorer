var argv = require('optimist').argv;
var async = require('async');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('../lib/MongoStore');

function main() {
  var netname = argv._[0];
  var txHash = new Buffer(argv._[1], 'hex');
  MongoStore.initialize([netname], function(err, netname) {
    if(err) throw err;
  }, function(err) {
    if(err) throw err;
    var store = MongoStore.stores[netname];
    var tx;
    async.series([
      function(c) {
	store.getTx(txHash, function(err, t){
	  if(err) return c(err);
	  if(!t) return c(new Error('No transaction found'));
	  tx = t;
	  c();
	});
      },
      function(c) {
	//store.setSpent([tx], c);
      },
    ], function(err) {
      if(err) throw err;
      process.exit();
    });
  });
}

main();
