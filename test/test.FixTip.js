var MongoStore = require('../lib/MongoStore2');

var netname = 'bitcoin';

MongoStore.initialize([netname], function(err, netname) {
  if(err) console.error(err);
  var store = MongoStore.stores[netname];
  var varCol = store.dbConn.collection('var');
  varCol.findOne({key:'tip'}, function(err, val) {
    console.log(val.blockHash.toString('hex'));
  });
});
