var bitcore = require('../alliances/bitcore/bitcore');
var Block = bitcore.Block;
var BlockReader = require('../lib/BlockReader');
var helper = require('../lib/helper');
var MongoStore = require('../lib/MongoStore2');
var Node = require('../lib/Node');
var async = require('async');

var blocks = [];
var blockObjs = [];
var path = '/home/fred/bitcoin/';
var netname = 'bitcoin';

function toBlockObj() {
  for(var i = 0; i < blocks.length; ++i) {
    var b = blocks[i];
    var blockObj = {
      hash : helper.reverseBuffer(b.calcHash(bitcore.networks[netname].blockHashFunc)),
      merkle_root : b.merkle_root,
      nonce : b.nonce,
      version : b.version,
      prev_hash : helper.reverseBuffer(b.prev_hash),
      timestamp : b.timestamp,
      bits : b.bits
    };
    blockObj.txes = b.txs.map(function(tx, idx) {
      return helper.processTx(netname, tx, idx, blockObj);
    });
    blockObjs.push(blockObj);
  }
}

function main() {
  // prepare test blocks
  var blockReader = new BlockReader(path, netname);
  var maxBlockNum = 2648;
  blockReader.readBlocks(171, function(err, vals) {
    for(var i = 0; i < vals.length; ++i) {
      blocks.push(vals[i].block);
    }
    toBlockObj();
    for(var i = 0; i < blockObjs.length; ++i) {
      var blockObj = blockObjs[i];
      //console.log(blockObj.txes[0]);
      if(blockObj.txes.length > 1) {
        console.log('idx=%d:hash=%s', i+1, blockObj.hash.toString('hex'));
        console.log(blockObj.txes.length);
      }
    }
    // prepare
    MongoStore.initialize([netname], function(err, netname) {
      var node = new Node(netname);
      var tasks = blockObjs.map(function(blockObj) {
        return function(c) {
          node.storeTipBlock(blockObj, true, function(err) {
            if(err) {
              console.log('block store fail:hash=%s', blockObj.hash.toString('hex'));
              return c(err);
            }
            console.log('block store success:hash=%s', blockObj.hash.toString('hex'));
            c();
          });
        };
      });
      async.series(tasks, function(err) {
        if(err) console.err(err.stack);
        var store = MongoStore.stores[netname];
        var txCol = store.dbConn.collection('tx');
        txCol.find({'vout.spent':{'$gt':0}}).toArray(function(err, vals) {
          for(var i = 0; i < vals.length; ++i) {
            console.log('spent > 0:' + vals[i].hash.toString('hex'));
          }
          store.dbConn.close();
        });
      });
    });
  });
}

main();
