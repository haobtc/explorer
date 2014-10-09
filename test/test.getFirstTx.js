var MongoStore = require('../lib/MongoStore2');
var stores = MongoStore.stores;

var netname = 'bitcoin';

MongoStore.initialize([netname], function(err, netname) {
  store = stores[netname];
  var txCol = store.dbConn.collection('tx');
  //txCol.findOne(function(err, tx) {
  txCol.find().sort('_id').limit(1).toArray(function(err, txes) {
    if(err) return console.error(err);
    console.log(txes[0]._id);
    console.log('sinceID=%s', txes[0].hash.toString('hex'));
    var sinceID = txes[0]._id;
    txCol.find({_id:{$gt:sinceID}}).sort('_id').limit(10).toArray(function(err, txes) {
      if(err) return console.error(err);
      txes.forEach(function(tx) {
        console.log(tx.hash.toString('hex'));
      });
      store.dbConn.close();
    });
  });
});

