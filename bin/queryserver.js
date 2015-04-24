var express = require('express');

var cluster = require('cluster');
var http = require('http');
var bitcore = require('bitcore-multicoin');
var config = require('../lib/config');
var async = require('async');
var app = express();
var bodyParser = require('body-parser');
var Query = require('../lib/Query');
var helper = require('../lib/helper');
var blockstore = require('../lib/blockstore');

app.use(bodyParser());

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
      var startTime = new Date();
      Query.getTxDetails(netname, hashList, function(err, txes) {
	if(err) return c(err);
	console.info('getTxDetails', netname, 'takes', (new Date() - startTime)/1000.0, 'secs');
	results = results.concat(txes||[]);
	c();
      });
    };
  }
  var tasks = helper.netnames().map(function(netname) {
    return getTx(netname);
  });

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

function getTxDetailsSinceID(req, res) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  
  var txlist = [];

  function txTask(netname) {
    return function(c) {
      var since = query.since;
      if(since) {
	since = new Buffer(since, 'hex');
      }
      Query.getTxListSinceId(netname, since, function(err, arr) {
	if(err) throw err;
	(arr||[]).forEach(function(tx) {
	  txlist.push(tx);
	});
	c();
      });
    };
  }

  var tasks = helper.netnames().map(function(netname) {
    return txTask(netname);
  });
  if(tasks.length > 0) {
    async.parallel(tasks, function(err) {
      if(err) throw err;
      sendJSONP(req, res, txlist);
    });
  } else {
    sendJSONP(req, res, []);
  }  
}
app.get('/queryapi/v1/tx/since', getTxDetailsSinceID);
app.post('/queryapi/v1/tx/since', getTxDetailsSinceID);

function getTxTimelineForNetwork(req, res) {
  var netname = req.params.netname;
  var since = req.query.since;
  var startTime = new Date();
  Query.getTxDetailsSinceID(netname, since, function(err, arr) {
    if(err) throw err;
    var txlist = arr || [];
    console.info('get timeline', req.params.netname, 'takes', (new Date() - startTime)/1000.0, 'secs');
    sendJSONP(req, res, txlist);
  });
}
app.get('/queryapi/v1/tx/:netname/timeline', getTxTimelineForNetwork);
app.get('/queryapi/v1/tx/:netname/since', getTxTimelineForNetwork);  // For backward compitable

app.get('/queryapi/v1/mempool/:netname/since', function(req, res) {
  res.send([]);
});

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
  var startTime = new Date();
  Query.getUnspent(addressList, function(err, results) {
    if(err) throw err;
    console.info('getUnspent takes', (new Date() - startTime)/1000.0, 'secs');
    sendJSONP(req, res, results);
  });
}
app.get('/queryapi/v1/unspent', getUnspent);
app.post('/queryapi/v1/unspent', getUnspent);

// Get unspent
function getTxList(req, res) {
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
  var startTime = new Date();
  Query.getTxListOfAddresses(addressList, query.detail == 'yes', function(err, results) {
    if(err) throw err;
    console.info('getTXList takes', (new Date() - startTime)/1000.0, 'secs');
    sendJSONP(req, res, results);
  });
}
app.get('/queryapi/v1/tx/list', getTxList);
app.post('/queryapi/v1/tx/list', getTxList);

function sendTx(req, res, next) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  var info = {};
  if(query.remote_address) {
    info.remoteAddress = query.remote_address;
  } else {
    info.remoteAddress = req.remoteAddress;
  }
  if(query.note) {
    info.note = query.note;
  }
  Query.addRawTx(req.params.netname, query.rawtx, info, function(err, ret) {
    if(err) {
      return next(err);
    }
    
    if(ret != undefined) {
      if(req.query.format == 'json') {
	sendJSONP(req, res, {'txid': ret});
      } else {
	res.send(ret);
      }
    } else {
      res.status(400).send({'error': 'Failed'});
    }
  });
}

app.get('/queryapi/v1/sendtx/:netname', sendTx);
app.post('/queryapi/v1/sendtx/:netname', sendTx);

function decodeRawTx(req, res, next) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  var tx = Query.decodeRawTx(req.params.netname, query.rawtx);
  var tTx = new blockstore.ttypes.Tx();
  tTx.netname(req.params.netname);
  tTx.fromTxObj(tx);
  sendJSONP(req, res, tTx.toJSON());
};

app.get('/queryapi/v1/decoderawtx/:netname', decodeRawTx);
app.post('/queryapi/v1/decoderawtx/:netname', decodeRawTx);

app.get('/:netname/nodes.txt', function(req, res) {
  var rpcClient = blockstore[req.params.netname];
  rpcClient.getPeers(function(err, peers) {
    res.set('Content-Type', 'text/plain');
    res.send(peers.join('\r\n'));
  });
});

app.get('/:netname/nodes.json', function(req, res) {
  var rpcClient = blockstore[req.params.netname];
  rpcClient.getPeers(function(err, peers) {
    res.send(peers);
  });
});

app.use(function(err, req, res, next){
  if(err instanceof helper.UserError) {
    res.status(400).send({code: err.code, error: err.message});
  } else {
    console.error('EEE', err.stack);
    res.status(500).send({error: err.message});
  }
});

function startServer(argv){
  var netnames = argv.c;
  if(typeof netnames == 'string') {
    netnames = [netnames];
  }
  netnames = netnames || helper.netnames();

  var server = http.Server(app);
  server.listen(argv.p || 9000, argv.h || '0.0.0.0');
}

module.exports.start = function(argv){
  var numWorkers = argv.n || 1;
  numWorkers = parseInt(numWorkers);
  if(cluster.isMaster) {
    console.info('start', numWorkers, 'workers');
    for(var i=0; i<numWorkers; i++) {
      cluster.fork();
    }
    cluster.on('exit', function(worker, code, signal) {
      console.log('work ' + worker.process.pid + ' died');
    });
  } else {
    startServer(argv);
  }
}
