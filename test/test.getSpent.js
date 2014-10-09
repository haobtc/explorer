var MongoStore = require('../lib/MongoStore2');
var stores = MongoStore.stores;

var netname = 'bitcoin';

MongoStore.initialize([netname], function(err, netname) {
  store = stores[netname];
  var txCol = store.dbConn.collection('tx');
  txCol.find({'vout.spent':{'$gt':1}}).toArray(function(err, txs) {
    if(txs.length !== 0) {
      console.log(txs[0].hash.toString('hex'));
      console.log(txs[0]);
    }
    store.dbConn.close();
  });
});
