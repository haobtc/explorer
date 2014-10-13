var bitcore = require('../alliances/bitcore/bitcore'),
     Block    = bitcore.Block,
     networks = bitcore.networks,
     Parser   = bitcore.BinaryParser,
     fs       = require('fs'),
     Buffer   = bitcore.Buffer,
     glob     = require('glob'),
     async    = require('async'),
     MongoStore = require('../lib/MongoStore2'),
     Node = require('../lib/Node'),
     helper   = require('../lib/helper');

var netname = 'bitcoin';
var path = '/home/yulei/bitcoin/blocks/blk*.dat';
var blocks = [];
var files = [];
var curFileIdx = 0;
var netWorkMagic = networks[netname].magic.toString('hex');
var totalBlockCnt = 0;
var store = null;
var node = null;

function toBlockObj(b) {
  var blockObj = {
    hash : helper.reverseBuffer(b.calcHash(bitcore.networks[netname].blockHashFunc)),
    merkle_root : b.merkle_root,
    nonce : b.nonce,
    version : b.version,
    prev_hash : helper.reverseBuffer(b.prev_hash),
    timestamp : b.timestamp,
    bits : b.bits,
    height : totalBlockCnt
  };
  blockObj.txes = b.txs.map(function(tx, idx) {
    var newTx = helper.processTx(netname, tx, idx, blockObj);
    if(newTx.bhash) {
      newTx.binfo = [{bhash:newTx.bhash, bidx:newTx.bidx}];
      delete newTx.bhash;
      delete newTx.bidx;
    }
    return newTx;
  });
  return blockObj;
}

/*function getMagic(parser) {
  var byte0 = parser.buffer(1).toString('hex');
  var p = parser.pos;
  var byte123 = parser.subject.toString('hex',p,p+3);
  var magic = byte0 + byte123;
  if(magic !== '00000000' && magic != netWorkMagic) {
    console.info('magic', magic);
    magic = null;
  }
  if(magic==='00000000') magic = null;
  return magic;
}*/

function flush2DB(cb) {
  var n = blocks.length;
  var i = 0;
  var blockCol = store.dbConn.collection('block');
  var txCol = store.dbConn.collection('tx');
  async.series([
    function(cc) {
      if(curFileIdx == 0) return cc();
      var prev_hash = blocks[0].prev_hash;
      blockCol.findAndModify({hash:prev_hash},
        [],
        {$set:{next_hash:blocks[0].hash}},
        {upsert: false, new: true},
        function(err) {
          if(err) return cc(err);
          cc();
        });
    },
    function(cc) {
      async.doWhilst(function(c) {
        var b = blocks[i];
        if(i < n-1) b.next_hash = blocks[i+1].hash;
        var txes = b.txes;
        delete b.txes;
        blockCol.findAndModify({hash:b.hash},
          [],
          {$set:b},
          {upsert: true, new: true},
          function(err, b) {
            if(err) return c(err);
            console.info('insert block:hash=%s', b.hash.toString('hex'));
            txCol.insert(txes, function(err) {
              if(err) return c(err);
              ++i;
              c();
            });
          });
      }, function() {
        return i < n;
      }, function(err) {
        if(err) console.error(err);
        cc();
      });
    }
  ],function(err) {
    if(err) return cb(err);
    cb(undefined);
  });
}

function handleOneFile(cb) {
  var s = new Date();
  var fname = files[curFileIdx];
  var stats = fs.statSync(fname);
  var size =  stats.size;
  console.log('Reading Blockfile %s [%d MB]', fname, parseInt(size/1024/1024));
  var fd = fs.openSync(fname, 'r');
  var buffer = new Buffer(size);
  fs.readSync(fd, buffer, 0, size, 0);
  var parser = new Parser(buffer);
  // read blocks
  var count = 0;
  while(!parser.eof()) {
    //var magic = getMagic(parser);
    parser.buffer(4+4);
    var b = new Block();
    b.parse(parser);
    b.getHash();
    var blockObj = toBlockObj(b);
    ++count;
    ++totalBlockCnt;
    blocks.push(blockObj);
  }
  console.info('load finish:fileIdx=%d:count=%d:cost=%d', curFileIdx, count, (new Date()-s)/1000);
  s = new Date();
  flush2DB(function(err) {
    if(err) return cb(err);
    console.info('import finish:fileIdx=%d:count=%d:cost=%d', curFileIdx, count, (new Date()-s)/1000);
    blocks = [];
  });
}

function main() {
  files = glob.sync(path);
  var n = files.length;
  n = 1;
  async.doWhilst(function(c) {
    handleOneFile(function(err) {
      if(err) return c(err);
      ++curFileIdx;
      c();
    });
  }, function() {
    return curFileIdx < n;
  }, function(err) {
    if(err) console.error(err);
    console.info('totalBlockCnt=%d', totalBlockCnt);
    store.dbConn.close();
  });
}

MongoStore.initialize([netname], function(err, netname) {
  if(err) console.error(err);
  node = new Node(netname);
  store = MongoStore.stores[netname];
  main();
});
