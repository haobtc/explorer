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

var blockCnt = 0;

function startImport(netname) {
  var path = '/home/fred/' + netname;
  var blockReader = new BlockReader(path, netname);
  var height = 0;
  var node = new Node(netname);
  var finish = false;
  var times = 0;
  var txCnt = 0;
  var onceReadBlockCnt = 2000;
  var start = new Date();
  async.doWhilst(function(c) {
    console.log('readBlocks');
    blockReader.readBlocks(onceReadBlockCnt, function(err, blocks) {
      if(err) return c(err);
      var tasks = blocks.map(function(b) {
        return function(c) {
          var blockObj = toBlockObj(b.block, height, netname);
          ++height;
          var tmp = blockObj.txes.length;
          node.storeTipBlock(blockObj, true, function(err) {
            if(err) return c(err);
            txCnt = txCnt + tmp;
            c();
          });
        };
      });
      async.series(tasks, function(err) {
        if(err) return c(err);
        ++times;
        blockCnt = blockCnt + blocks.length;
        if(blockCnt % 1000 == 0) console.log('blockCnt=%d:cost=%d', blockCnt, (new Date() - start)/1000);
        if(blocks.length !== onceReadBlockCnt) finish = true;
        c();
      });
    });
  },
  function() {
    if(finish) console.log('finished:blocksCnt=%d:txCnt=%d', blockCnt, txCnt);
    return !finish;
  },
  function(err) {
    if(err) console.error(err);
    MongoStore.stores[netname].dbConn.close();
  });
}

MongoStore.initialize(['bitcoin'], function(err, netname) {
  startImport(netname);
});

