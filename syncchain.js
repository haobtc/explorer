var bitcore = require('./alliances/bitcore/bitcore');
var ChainSync = require('./lib/ChainSync');
var MongoStore = require('./lib/MongoStore');
var helper = require('./lib/helper');

Object.prototype.clone = function(deep) {
  deep = deep || false;
  var n = new Object();
  for(var key in this) {
    if(deep) {
      n[key] = this[key].clone(deep);
    } else {
      n[key] = this[key];
    }
  }
  return n;
}

MongoStore.initialize(['dogecoin'], function(err, netname) {
  var store = MongoStore.stores[netname];
/*  var genesisBlock = bitcore.networks[netname].genesisBlock.clone();
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

