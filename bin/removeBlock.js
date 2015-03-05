var argv = require('optimist').argv;
var async = require('async');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('../lib/MongoStore');


function main() {
  var netname = argv._[0];
  var bHash = new Buffer(argv._[1], 'hex');
  MongoStore.initialize([netname], function(err, netname) {
    if(err) throw err;
  }, function(err) {
    if(err) throw err;
    var store = MongoStore.stores[netname];
    var block;
    async.series([
      function(c) {
	store.getBlock(bHash, function(err, b) {
	  if(err) return c(err);
	  if(b.isMain) return c(new Error('Block is Main, cannot be removed'));
	  block = b;
	  c();
	});
      },
      function(c) {
	store.removeBlock(block, c);
      }
    ], function(err) {
      if(err) throw err;
    });
  });
}

main();
