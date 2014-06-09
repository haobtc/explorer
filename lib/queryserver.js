var express = require('express');
var _ = require('underscore');
var bitcore = require('../alliances/bitcore/bitcore');
var config = require('./config');
var async = require('async');
var app = express();
var bodyParser = require('body-parser');
var Query = require('./query');
var MongoStore = require('./MongoStore');
var helper = require('./helper');

app.use(bodyParser());
app.use(function(err, req, res, next){
  console.error(err.stack);
  res.send({error: true});
});

app.use('/explorer/', express.static('public'));

function sendJSONP(req, res, obj) {
    if(req.query.callback && /^\w+$/.test(req.query.callback)) {
        res.set('Content-Type', 'text/javascript');
        res.send(req.query.callback + '(' + JSON.stringify(obj) + ');');
    } else {
        res.set('Content-Type', 'application/json');
        res.send(obj);
    }
}

// Get TxDetails
function getTxDetails(req, res) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  var results = [];
  function getTx(netname) {
    return function(c) {
      if(!query[netname]) {
        return c();
      }
      var hashList = query[netname].split(',');
      if(hashList.length == 0) return c();
      hashList = hashList.map(function(hash) {return new Buffer(hash, 'hex');});
      var store = MongoStore.stores[netname];
      Query.getTxDetails(store, hashList, function(err, txes) {
	if(err) return c(err);
	results = results.concat(txes||[]);
	c();
      });
    };
  }
  var tasks = [];
  for(var netname in MongoStore.stores) {
    tasks.push(getTx(netname));
  }

  if(tasks.length > 0) {
    async.parallel(tasks, function(err) {
      if(err) throw err;
      sendJSONP(req, res, results);      
    });
  } else {
    sendJSONP(req, res, []);
  }
};
app.get('/queryapi/v1/tx/details', getTxDetails);
app.post('/queryapi/v1/tx/details', getTxDetails);

// Get unspent
function getUnspent(req, res) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  if(!query.addresses) {
    return sendJSONP(req, res, []);
  }

  var addressList = [];
  query.addresses.split(',').forEach(function(addrStr) {
    var addr = new bitcore.Address(addrStr);
    if(addr.isValid()) {
      addressList.push(addr);
    }
  });
  if(addressList.length == 0) {
    return sendJSONP(req, res, []);
  }
  Query.getUnspent(addressList, function(err, results) {
    if(err) throw err;
    sendJSONP(req, res, results);
  });
}
app.get('/queryapi/v1/unspent', getUnspent);
app.post('/queryapi/v1/unspent', getUnspent);


function sendTx(req, res) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  var rpcConfig = _.clone(config.rpc);
  rpcConfig.user = req.params.netname;
  var rpc = new bitcore.RpcClient(rpcConfig);
  rpc.sendRawTransaction(query.rawtx, function(err, ret) {
    if(err) throw err;
    console.info(ret);
    if(ret) {
      res.send(JSON.stringify(ret));
    } else {
      res.status(400).send('Failed');
    }
  });
}
app.get('/queryapi/v1/tx/send/:netname', sendTx);
app.post('/queryapi/v1/tx/send/:netname', sendTx);

module.exports.start = function(argv){
  var coins = argv.c;
  if(typeof coins == 'string') {
    coins = [coins];
  }
  MongoStore.initialize(coins || helper.netnames());
  app.listen(argv.p || 9000);
}
