var bitcore = require('../alliances/bitcore/bitcore');
var Block = bitcore.Block;
var Script = bitcore.Script;
var BlockReader = require('../lib/BlockReader');
var helper = require('../lib/helper');
var MongoStore = require('../lib/MongoStore2');
var Node = require('../lib/Node');
var async = require('async');


function toBlockObj(b, height) {
    var blockObj = {
      hash : helper.reverseBuffer(b.calcHash(bitcore.networks[netname].blockHashFunc)),
      merkle_root : b.merkle_root,
      nonce : b.nonce,
      version : b.version,
      prev_hash : helper.reverseBuffer(b.prev_hash),
      timestamp : b.timestamp,
      height : height,
      bits : b.bits
    };
    blockObj.txes = b.txs.map(function(tx, idx) {
      return helper.processTx(netname, tx, idx, blockObj);
    });
    return blockObj;
}

var netname = 'bitcoin';
var path = '/home/fred/bitcoin/';
var blockObjs = [];

MongoStore.initialize([netname], function(err, netname) {
  if(err) return console.error(err);
  var blockReader = new BlockReader(path, netname);
  var store = MongoStore.stores[netname];
  var node = new Node(netname);
  blockReader.readBlocks(171, function(err, vals) {
    for(var i = 0; i < vals.length; ++i) {
      blockObjs.push(toBlockObj(vals[i].block, i));
    }
    var tasks = blockObjs.map(function(b) {
      return function(c) {
        node.storeTipBlock(b, true, function(err) {
          if(err) console.error(err);
          c();
        });
      };
    });
    async.series(tasks, function(err) {
      if(err) console.error(err);
      store.dbConn.close();
    });
  });
});
