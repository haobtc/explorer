var mongodb = require('mongodb');
var helper = require('./helper');
var stores = {};

module.exports.initialize = function(netnames, callback) {
  netnames.forEach(function(netname) {
    var store = Store(netname);
    stores[netname] = store;
    store.connect(function(err, conn) {
      if(typeof callback=='function') callback(err, netname);
    });
  });
};

function Store(netname) {
  var store = {};
  var conn;

  store.connect = function(callback) {
    var url = "mongodb://localhost:27017/blocks_" + netname;
    mongodb.MongoClient.connect(url, function(err, aConn) {
      if(err) {
        console.error(err);
	if(typeof callback == 'function') callback(err);
        return;
      }
      conn = aConn;

      function nf(err) {if(err){console.error(err);}}

      var blockCol = conn.collection('block');
      blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
      blockCol.ensureIndex({'height': -1}, nf);

      var txCol = conn.collection('tx');
      txCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
      txCol.ensureIndex({'bhash': 1}, {}, nf);
      txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1}, {unique: 1, sparse: 1}, nf);
      txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

      if(typeof callback == 'function') callback(conn);
    });
  };

  store.conn = function() {
    return conn;
  };
  return store;
}
module.exports.stores = stores;
