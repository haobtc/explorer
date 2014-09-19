var fs = require('fs');
var mongodb = require('mongodb');
var MongoStore = require('../lib/MongoStore2');
var bitcore = require('../alliances/bitcore/bitcore');
var helper = require('../lib/helper');
var Block = bitcore.Block;
var BinaryParser = bitcore.BinaryParser;
var stores = MongoStore.stores;

var netname = 'bitcoin';
var store;

function getBlock() {
  var b = new Block();
  var buffer = new Buffer(fs.readFileSync('blk86756-testnet.dat'));
  var p = new BinaryParser(buffer);
  var magic = p.buffer(4).toString('hex');
  p.word32le();
  b.parse(p, false);
  return b;
}

console.log('start');

MongoStore.initialize([netname], function(err, netname) {
  console.log('success');
  store = stores[netname];
  var block = getBlock();
  var blockObj = {
    hash: helper.reverseBuffer(block.calcHash(bitcore.networks[netname].blockHashFunc)),
    merkle_root: block.merkle_root,
    height: 0,
    nonce: block.nonce,
    version: block.version,
    prev_hash: helper.reverseBuffer(block.prev_hash),
    timestamp: block.timestamp,
    bits: block.bits
  };

  blockObj.txes = block.txs.map(function(tx, idx) {
    return helper.processTx(netname, tx, idx, blockObj);
  });

  blockObj.isMain = true;

  /*
  // test insertBlock
  store.insertBlock(blockObj, function(err, txes) {
    console.log(txes.length);

    // test getBlock
    store.getBlock(blockObj.hash, function(err, block) {
      console.log('get block:hash=%s', block.hash.toString('hex'));

      // test saveBlock
      block.isMain = false;
      store.saveBlock(block, function(err) {
        store.dbConn.close();
      });
    });
  });
  */
  store.setTipBlock(blockObj, function(err) {
    if(err) console.error(err.message);
    store.getTipBlock(function(err, v) {
      if(err) console.error(err.message);
      console.log(v);
      store.dbConn.close();
    });
  });
});
