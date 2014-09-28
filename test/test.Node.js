var bitcore = require('../alliances/bitcore/bitcore');
var Block = bitcore.Block;
var Script = bitcore.Script;
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
    }
    // prepare
    MongoStore.initialize([netname], function(err, netname) {
      var node = new Node(netname);
      var tasks = blockObjs.map(function(blockObj,i) {
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
      var newTasks = [];
      //for(var i = 0; i < tasks.length; ++i) newTasks.push(tasks[i]);
      async.series(tasks, function(err) {
        if(err) return console.error(err.stack);
        MongoStore.stores[netname].dbConn.close();
        /*var newTx = blockObjs[170].txes[1];
        node.sendTxTest(newTx, function(err, txHash) {
          if(err) return console.err(err.stack);
          console.log('txHash=%s', txHash);
          async.series([tasks[9]], function(err) {
            if(err) console.error(err);
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
        */
      });
    });
  });
}

main();
