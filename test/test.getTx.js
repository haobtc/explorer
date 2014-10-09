var MongoStore = require('../lib/MongoStore2');
var stores = MongoStore.stores;

var netname = 'bitcoin';

MongoStore.initialize([netname], function(err, netname) {
  store = stores[netname];
  var txCol = store.dbConn.collection('tx');
  var key1 = '026e5a5d362cb4ea40c73e38d7512bce9742b13ab9dd78b4654607f1723e62e1';
  var key2 = '447c8d3c215deedbdb4c1f95fa5c44d71fb5f7c82ed9b7550a9a021b86c463e6';
  var hash1 = new Buffer(key1, 'hex');
  var hash2 = new Buffer(key2, 'hex');
  txCol.find({hash:{$in:[hash1,hash2]}}).toArray(function(err, docs) {
    console.log('0=%s', docs[0].hash.toString('hex'));
    console.log(docs[0]);
    console.log('1=%s', docs[1].hash.toString('hex'));
    console.log(docs[1]);
    var b = docs[0]._id > docs[1]._id;
    console.log(docs[0]._id.getTimestamp(), docs[1]._id.getTimestamp());
    console.log(b);
    store.dbConn.close();
  });
});

