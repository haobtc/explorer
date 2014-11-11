var express = require('express');

var cluster = require('cluster');
var http = require('http');
var bitcore = require('bitcore-multicoin');
var config = require('../lib/config');
var async = require('async');
var app = express();
var bodyParser = require('body-parser');
var Query = require('../lib/Query');
//var Stream = require('../lib/Stream');
var MongoStore = require('../lib/MongoStore');
var helper = require('../lib/helper');

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
      var store = MongoStore.stores[netname];
      var startTime = new Date();
      Query.getTxDetails(store, hashList, function(err, txes) {
	if(err) return c(err);
	console.info('getTxDetails', netname, 'takes', (new Date() - startTime)/1000.0, 'secs');
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

function getTxDetailsSinceID(req, res) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  
  var txlist = [];

  function txTask(netname) {
    return function(c) {
      var since = query.since;
      var store = MongoStore.stores[netname];
      Query.getTxDetailsSinceID(store, since, function(err, arr) {
	if(err) throw err;
	//sendJSONP(req, res, arr || []);
	(arr||[]).forEach(function(tx) {
	  txlist.push(tx);
	});
	c();
      });
    };
  }

  var tasks = [];
  for(var netname in MongoStore.stores) {
    tasks.push(txTask(netname));
  }
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
function getTXList(req, res) {
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
  Query.getTXList(addressList, {}, query.detail == 'yes', function(err, results) {
    if(err) throw err;
    console.info('getTXList takes', (new Date() - startTime)/1000.0, 'secs');
    sendJSONP(req, res, results);
  });
}
app.get('/queryapi/v1/tx/list', getTXList);
app.post('/queryapi/v1/tx/list', getTXList);

function sendTx(req, res, next) {
  var query = req.query;
  if(req.method == 'POST') {
    query = req.body;
  }
  Query.addRawTx(req.params.netname, query.rawtx, function(err, ret) {
    if(err) {
      console.error('hhhh', err);
      return next(err);
    }
    
    if(ret != undefined) {
      console.info('send', ret);
      res.send(ret);
    } else {
      res.status(400).send('Failed');
    }
  });
}

app.get('/queryapi/v1/sendtx/:netname', sendTx);
app.post('/queryapi/v1/sendtx/:netname', sendTx);

/*Stream.addRPC('sendtx', function(rpc, network, rawtx) {
  Query.addRawTx(network, rawtx, function(err, txid) {
    if(err) {
      if(err instanceof helper.UserError) {
	rpc.send({code: err.code, 'error': err.message});
      } else {
	rpc.send({'error': err.message});
      }
    } else {
      rpc.send({'txid': txid});
    }
  });
});
*/

app.get('/:netname/nodes.txt', function(req, res) {
  var store = MongoStore.stores[req.params.netname];
  store.getVar('peers.' + req.params.netname, function(err, v) {
    res.set('Content-Type', 'text/plain');
    res.send(v.peers.join('\r\n'));
  });
});

app.get('/:netname/nodes.json', function(req, res) {
  var store = MongoStore.stores[req.params.netname];
  store.getVar('peers.' + req.params.netname, function(err, v) {
    res.send(v.peers);
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
  MongoStore.initialize(netnames, function(err, netname) {
    if(err) throw err;
  }, function() {
    //Stream.createStream(server, netnames);
  });
  server.listen(argv.p || 9000);
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
