var express = require('express');
var bitcore = require('bitcore-multicoin');
var MongoStore = require('../lib/MongoStore');
var helper = require('../lib/helper');
var config = require('../lib/config');
var BlockFetcher = require('../lib/BlockFetcher');

module.exports.start = function(argv){
  var netname = argv.c;
  var blockHash = new Buffer(argv.b, 'hex');
  
  MongoStore.initialize([netname], function(err, netname) {
    if(err) throw err;
  }, function(err) {
    if(err) throw err;
    var fetcher = new BlockFetcher(netname);
    fetcher.run(function(){
      fetcher.fetchBlock(blockHash, function(blockObj) {
	console.info('got block w/', blockObj.txes.length, 'txes');
	fetcher.blockChain.updateBlock(blockObj, function(err) {
	  if(err) {
	    console.info(err);
	    throw err;
	  }
	  console.info('updated');
	  process.exit();
	});
      });
    });
  });
};
