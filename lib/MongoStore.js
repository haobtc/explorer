var mongodb = require('mongodb');
var helper = require('./helper');
var config = require('./config');
var stores = {};

module.exports.initialize = function(netnames, callback) {
  netnames.forEach(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    store.connect(function(err, conn) {
      if(typeof callback=='function') callback(err, netname);
    });
  });
};

function Store(netname) {
  this.netname = netname;
}

Store.prototype.connect = function(callback) {
  var self = this;
  var url = config[this.netname].db.url;
  mongodb.MongoClient.connect(url, function(err, aConn) {
    if(err) {
      console.error(err);
      if(typeof callback == 'function') callback(err);
      return;
    }
    self.dbconn = aConn;
    
    function nf(err) {if(err){console.error(err);}}

    var blockCol = self.dbconn.collection('block');
    blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    blockCol.ensureIndex({'height': -1}, nf);

    var txCol = self.dbconn.collection('tx');
    txCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    txCol.ensureIndex({'bhash': 1}, {}, nf);
    txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1}, {unique: 1, sparse: 1}, nf);
    txCol.ensureIndex({'vin.spt': 1}, {}, nf);
    txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    var mpCol = self.dbconn.collection('mempool');
    mpCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
    mpCol.ensureIndex({'vin.hash': 1, 'vin.n': 1}, {unique: 1, sparse: 1}, nf);
    mpCol.ensureIndex({'vout.addrs': 1}, {}, nf);

    if(typeof callback == 'function') callback(undefined, self.dbconn);
  });
};

Store.prototype.conn = function() {
  return this.dbconn;
};

module.exports.stores = stores;
