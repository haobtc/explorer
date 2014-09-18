var fs = require('fs');
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

MongoStore.initialize([netname], function(err, netname) {
  console.log('success');
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

  var self = this;

  blockObj.txes = block.txs.map(function(tx, idx) {
    return helper.processTx(netname, tx, idx, blockObj);
  });

  block.isMain = true;

  store = stores[netname];
  // test insertBlock

  store.dbConn.close();
});
