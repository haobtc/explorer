var express = require('express');
var bodyParser = require('body-parser');
var basicAuth = require('basic-auth');
var bitcore = require('../alliances/bitcore/bitcore');
var MongoStore = require('./MongoStore');
var Node = require('./Node');
var helper = require('./helper');
var config = require('./config');

var app = express();
var p2pNodes = {};

app.use(bodyParser());
app.use(function(err, req, res, next){
    console.error(err.stack);
    res.send({error: true});
});

var rpcMethods = {};
rpcMethods.decoderawtransaction = function(req, res) {
  var rawtx = req.body.params[0];
  var parser = new bitcore.BinaryParser(new Buffer(rawtx, 'hex'));
  var tx = new bitcore.Transaction();
  tx.parse(parser);
  if(tx.serialize().toString('hex') == rawtx) {
    console.info('yes');
  }
  var stx = tx.getStandardizedObject();
  res.send(JSON.stringify(stx));
};

rpcMethods.sendrawtransaction = function(req, res) {
  var rawtx = req.body.params[0];
  var parser = new bitcore.BinaryParser(new Buffer(rawtx, 'hex'));
  var tx = new bitcore.Transaction();
  tx.parse(parser);
  if(tx.serialize().toString('hex') != rawtx) {
    res.status(400).send(JSON.stringify('TX rejected'));
  } else {
    p2pNodes[req.netname].sendTx(tx, function(err, r) {
      if(err) throw err;
      res.send(JSON.stringify(r));
    });
  }
};

app.post('/', function(req, res, next) {
  var user = basicAuth(req);
  if(!!config.networks[user.name] &&
     user.pass == config.rpc.pass) {
    req.netname = user.name;
    next();
  } else {
    res.set('WWW-Authenticate', 'Basic realm="Openblock"');
    res.status(401).send('Auth failed.');
  }
});

app.post('/', function(req, res) {
  var rpc = rpcMethods[req.body.method];
  if(rpc) {
    rpc(req, res);
  } else {
    res.send({code: -1, message:'method ' + rpc.body.method + ' not supported'});
  }
});

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  MongoStore.initialize(coins || helper.netnames(), function(err, netname){
    var node = new Node(netname);
    p2pNodes[netname] = node;
    node.updateMempool = true;
    node.synchronize = true;
    node.allowOldBlock = false;
    node.run();
  });
  app.listen(argv.p || config.rpc.port);
};
