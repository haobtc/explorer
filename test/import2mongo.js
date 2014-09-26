var bitcore = require('../alliances/bitcore/bitcore');
var Block = bitcore.Block;
var BlockReader = require('../lib/BlockReader');
var helper = require('../lib/helper');
var MongoStore = require('../lib/MongoStore2');
var Node = require('../lib/Node');
var async = require('async');

function toBlockObj(b, height, netname) {
  var blockObj = {
    hash : helper.reverseBuffer(b.calcHash(bitcore.networks[netname].blockHashFunc)),
    merkle_root : b.merkle_root,
    nonce : b.nonce,
    version : b.version,
    prev_hash : helper.reverseBuffer(b.prev_hash),
    timestamp : b.timestamp,
    bits : b.bits,
    height : height
  };
  blockObj.txes = b.txs.map(function(tx, idx) {
    return helper.processTx(netname, tx, idx, blockObj);
  });
  return blockObj;
}

function startImport(netname) {
  var path = '/home/fred/' + netname;
  var blockReader = new BlockReader(path, netname);
  var height = 0;
  var node = new Node(netname);
  var cblock;
  var times = 0;
  async.doWhilst(function(c) {
    blockReader.readBlocks(100, function(err, blocks) {
      if(err) return c(err);
      var tasks = blocks.map(function(b) {
        return function(c) {
          var blockObj = toBlockObj(b.block, height, netname);
          ++height;
          node.storeTipBlock(blockObj, true, function(err) {
            if(err) return c(err);
            cblock = b;
            c();
          });
        };
      });
      async.series(tasks, function(err) {
        if(err) return c(err);
        ++times;
        c();
      });
    });
  },
  function() {
    return cblock && times < 2000;
  },
  function(err) {
    if(err) console.error(err);
    MongoStore.stores[netname].dbConn.close();
  });
}

MongoStore.initialize(['bitcoin'], function(err, netname) {
  startImport(netname);
});

