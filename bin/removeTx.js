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
	store.getBlocks([tx], function(err) {
	  if(err) return c(err);
	  if(tx.blocks &&
	     tx.blocks.length > 0) {
	    console.info('Tx is in blocks', tx.blocks.map(function(b){return b.hash.toString('hex');}));
	    return c(new Error('Tx is in blocks'));
	  }
	  c();
	});	
      },
      function(c) {
	var col = store.dbconn.collection('tx');
	col.find({'vin.hash': tx.hash}).toArray(function(err, arr) {
	  if(err) return c(err);
	  if(arr.length > 0) {
	    var requiree = arr.map(function(t) { return t.hash.toString('hex');});
	    console.error('Tx required by other txes', requiree);
	    return c(new Error('tx is required by other txes, so it cannot be deleted'));
	  }
	  return c();
	});
      },
      function(c) {
	store.removeTxes([tx], function(err) {
	  if(err) return c(err);
	  console.info('removed', tx);
	  c();
	});
      }
    ], function(err) {
      if(err) throw err;
    });
  });
}

main();
