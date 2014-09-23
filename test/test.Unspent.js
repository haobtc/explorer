var MongoStore = require('../lib/MongoStore2');
var stores = MongoStore.stores;

var netname = 'bitcoin';

MongoStore.initialize([netname], function(err, netname) {
  store = stores[netname];
  var txCol = store.dbConn.collection('tx');
  txCol.find({'vout.spent':{'$gt':0}}).toArray(function(err, txs) {
    var voutCount = 0;
    for(var i = 0; i < txs.length; ++i) {
      var tx = txs[i];
      for(var j = 0; j < tx.vout.length; ++j) {
        if(tx.vout[j].spent > 0) {
          voutCount++;
        }
      }
    }
    console.log('voutCount=%d', voutCount);
    txCol.find({'vin.v':{'$exists':true}}).toArray(function(err, txes) {
      var count = 0;
      for(var i = 0; i < txes.length; ++i) {
        var t = txes[i];
        for(var j = 0; j < t.vin.length; ++j) {
          if(t.vin[j].v) {
            ++count;
          }
        }
      }
      console.log('count=%d', count);
    });
  });
});
