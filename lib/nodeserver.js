var express = require('express');
var bodyParser = require('body-parser');
var basicAuth = require('basic-auth');
var bitcore = require('../alliances/bitcore/bitcore');
var NodeSet = require('./NodeSet');
var helper = require('./helper');
var config = require('./config');

var app = express();

var nodeSet = new NodeSet();

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
    nodeSet.node(req.netname).sendTx(tx, function(err, r) {
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
  var sync = true;
  var blockHash;
  if(argv.b) {
    sync = false;
    blockHash = new Buffer(argv.b, 'hex');
    console.info(blockHash.toString('hex') == argv.b);
  }
  nodeSet.run(coins||helper.netnames(), function(node) {
    node.updateMempool = true;
    node.allowOldBlock = false;
    if(sync) {
      node.synchronize = true;
    } else {
      node.synchronize = false;
      node.addBlocks = false;
      node.fetchBlock(blockHash, function(blockObj) {
	console.info('got block', blockObj.txes.length);
	node.updateBlock(blockObj, function(err) {
	  if(err) {
	    console.info(err);
	    throw err;
	  }
	  console.info('updated');
	  process.exit();
	});
      });
    }
  }, function(err) {
    if(err) throw err;
    function stopNode() {
      console.log('stopping node server\n');
      nodeSet.stop(function(){
	console.log('stopped\n');
	process.exit();
      });
    }
    process.on('SIGINT', stopNode);
    process.on('SIGTERM', stopNode);
    if(argv.r) {
      var runsecs = parseInt(argv.r);
      if(!isNaN(runsecs)) {
	setTimeout(stopNode, runsecs * 1000);
      }
    }
  });
  app.listen(argv.p || config.rpc.port);
};
