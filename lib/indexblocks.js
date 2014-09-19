var fs = require('fs');
var async = require('async');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('./MongoStore');
var Node = require('./Node');
var helper = require('./helper');
var config = require('./config');
var BlockReader = require('./BlockReader');

MongoStore.initialize(['bitcoin'] || helper.netnames(), function(err, netname){
  startIndex(netname);
});

function startIndex(netname) {
  var be = new BlockReader('/home/zengke/.' + netname, bitcore.networks[netname]);
  var cblock;
  var times = 0;
  var store = MongoStore.stores[netname];
  var col = store.dbconn.collection('blockindex');
  be.rewind(45, 0);
  async.doWhilst(function(c) {
    be.readBlocks(100, function(err, blocks) {
      if(err) return c(err);
      times++;
      cblock = null;
      blocks.forEach(function(b) {
	var bindex = {hash: helper.reverseBuffer(b.block.hash),
		      prev_hash: helper.reverseBuffer(b.block.prev_hash),
		      fileIndex: b.fileIndex,
		      pos: b.pos};
	col.update({hash: bindex.hash},
		   {$set: bindex}, {upsert: true}, function(err) {
		     c(err);
		   });
	cblock = b;
      });      
      if(cblock) {
	console.info(helper.reverseBuffer(cblock.block.hash).toString('hex'), cblock.fileIndex, cblock.pos, times);
      }
    });
  }, function() {
    return cblock && times < 2000;
  }, function(err) {
    if(err) throw err;
  });
  
}
