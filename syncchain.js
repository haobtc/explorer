var bitcore = require('./alliances/bitcore/bitcore');
var ChainSync = require('./controllers/ChainSync');
var MongoStore = require('./lib/MongoStore');
var helper = require('./lib/helper');


MongoStore.initialize(['dogecoin'], function(err, netname) {
/*  var genesisBlock = helper.clone(bitcore.networks[netname].genesisBlock);
  genesisBlock.hash = helper.reverseBuffer(genesisBlock.hash);
  genesisBlock.prev_hash = helper.reverseBuffer(genesisBlock.prev_hash);
  genesisBlock.txes = [];
*/

/*  store.getBlock(genesisBlock.hash, function(err, block) {
    if(!block) {
      //    store.addBlock(genesisBlock);
      store.pipedPushBlock(genesisBlock, function() {
      });
    }
  }); */

  var chainSync = new ChainSync(netname);
  chainSync.run();
});

