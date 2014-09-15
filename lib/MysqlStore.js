// mysql CRUD
var mysql = require('mysql');
var config = require('../config');

var stores = {};

module.exports.initialize = function(netnames, callback, complete) {
  var tasks = netnames.map(function(netname) {
    var store = new Store(netname);
    stores[netname] = store;
    return function(c) {
      store.connect(function(err, conn) {
        if(err) c(err);
        if(typeof callback === 'function') callback(err, netname);
        c(err, netname);
      });
    };
  });
  async.parallel(tasks, complete);
};

function Store(netname) {
  this.netname = netname;
}

Store.prototype.connect = function(cb) {
  var self = this;
  var connection = this._createMySqlPool(netname);
  connection.connect(function(err) {
    if(err) {
      console.error(err);
      if(typeof callback === 'function') return cb(err);
    }
    self.dbConn = connecton;
  });
};

Store.prototype.insert = function(sql, args, cb) {
  this.query(sql, args, db);
};

Store.prototype.delete = function(sql, args, cb) {
  this.query(sql, args, db);
};

Store.prototype.update = function(sql, args, cb) {
  this.query(sql, args, db);
};

Store.prototype._createMySqlPool = function(netname, url)
