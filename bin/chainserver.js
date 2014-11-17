var express = require('express');
var async = require('async');
var http = require('http');
var bitcore = require('bitcore-multicoin');
var BlockChain = require('../lib/BlockChain');
var MongoStore = require('../lib/MongoStore');
var helper = require('../lib/helper');

var app = express();

app.use(function(req, res, next) {
  if(req.method == 'POST') {
    req.rawBody = '';
    req.setEncoding('utf8');
    req.on('data', function(chunk) { 
      req.rawBody += chunk;
    });

    req.on('end', function() {
      next();
    });
  } else {
    next();
  }
});

var bodyParser = require('body-parser');
app.use(bodyParser({limit: '50mb'}));

var blockChains = {};

app.get('/stats/:netname', function(req, res) {
  var blockChain = blockChains[req.params.netname];
  res.send({
    'queueSize':  blockChain.blockQueue.size(),
    'tipTimestamp': blockChain.store.stats.tipTimestamp,
    'maxHeight': blockChain.store.stats.maxHeight
  });
});

app.post('/blocks/:netname', function(req, res, next) {
  var self = this;
  var blockChain = blockChains[req.params.netname];
  var parser = new bitcore.BinaryParser(new Buffer(req.rawBody, 'binary'));
  var block = new bitcore.Block();
  block.parse(parser);

  var blockHash = block.calcHash(bitcore.networks[req.params.netname].blockHashFunc);
  var blockHashHex = blockHash.toString('hex');
  //console.info('on block hash hex', blockHashHex, 'rawsize', req.body.raw.length);

  var blockObj = {
    hash: helper.reverseBuffer(blockHash),
    merkle_root: block.merkle_root,
    height: 0,
    nonce: block.nonce,
    version: block.version,
    prev_hash: helper.reverseBuffer(block.prev_hash),
    timestamp: block.timestamp,
    bits: block.bits
  };
  blockObj.txes = block.txs.map(function(tx, idx) {
    return helper.processTx(blockChain.netname, tx, idx, blockObj);
  });
  blockObj.cntTxes = blockObj.txes.length;
  blockChain.enqueueBlock({blockObj:blockObj});
  res.send('ok');
});

app.use(function(err, req, res, next){
  if(err instanceof helper.UserError) {
    res.status(400).send({code: err.code, error: err.message});
  } else {
    console.error('EEE', err.stack);
    res.status(500).send({error: err.message});
  }
});

var stop = module.exports.stop = function(cb) {
  function stopChainFn(blockChain) {
    return function(c) {
      blockChain.stop(c);
    };
  }
  var fns = [];
  for(var netname in blockChains) {
    var blockChain = blockChains[netname];
    fns.push(stopChainFn(blockChain));
  }
  async.parallel(fns, cb);
};

module.exports.start = function(argv) {
  var netnames = argv.c;
  if(typeof netnames == 'string') {
    netnames = [netnames];
  }
  netnames = netnames || helper.netnames();

  MongoStore.initialize(helper.netnames(), function(err, netname) {
    blockChains[netname] = new BlockChain(netname);
  }, function(err) {
    function stopChains() {
      stop(function(err) {
	if(err) throw err;
	console.info('chain server stoped');
	process.exit();
      });
    }
    process.on('SIGINT', stopChains);
    process.on('SIGTERM', stopChains);
  });
  
  var server = http.Server(app);
  server.listen(parseInt(argv.p || 19000), argv.h || 'localhost');
};
