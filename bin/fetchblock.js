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
    fetcher.start(blockHash);
  });
};
