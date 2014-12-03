var bitcore = require('bitcore-multicoin');
var async = require('async');
var MongoStore = require('./MongoStore');
var util = bitcore.util;
var helper = require('./helper');
var config = require('./config');
var Peer = bitcore.Peer;
var PeerManager = bitcore.PeerManager;
var Script = bitcore.Script;
var buffertools = require('buffertools');
var Queue = require('./queue');
var Skip = helper.Skip;

var hex = function(hex) {return new Buffer(hex, 'hex');};

function BlockChain(netname) {
  var self = this;
  this.netname = netname;
  this.blockQueue = new Queue({timeout:600000, limit:1810});
  this.store = MongoStore.stores[netname];
}

BlockChain.prototype.start = function() {
  //this.insertTxes();
};


BlockChain.prototype.insertTxes = function() {
  var self = this;
  var col = this.store.dbconn.collection('tmptx');
  async.whilst(function() {
    return true;
  }, function(c) {
    col.findAndModify({processed: {$exists: false}}, [],
		      {$set: {processed: true}},
		      {upsert: false, new: true},
		      function(err, txObj) {
			if(err) return c(err);
			if(txObj) {
			  txObj.hash = helper.toBuffer(txObj.hash);
			  txObj.bhash = helper.toBuffer(txObj.bhash);
			  delete txObj._id;
			  self.store.takeInputAddresses(txObj, function(err) {
			    if(err) return c(err);
			    console.info(new Date(), 'starting add txes');
			    self.store.addTxes([txObj], c);
			  });

			} else {
			  col.remove({processed: true}, function(err) {
			    if(err) return c(err);
			    setTimeout(c, 3000);
			  });
			}
		      });
  }, function(err) {
    if(err) throw err;
  });
};

BlockChain.prototype.insertBlock = function(block, callback) {
  var self = this;
  block.isMain = true;
  var col = this.store.dbconn.collection('block');
  //var tmptxcol = this.store.dbconn.collection('tmptx');

  if(typeof callback != 'function') {
    callback = function(err) {if(err){console.error(err.stack)}};
  }
  delete block._id;
  var txes = block.txes;
  delete block.txes;

  var startDate = new Date();
  txes = txes.map(function(tx, idx) {
    return helper.processTx(self.netname, tx, idx, block);
  });
  console.info('process txes', txes.length, 'using', (new Date() - startDate)/1000, 'secs');

  console.info('save block', this.netname, block.hash.toString('hex'), 'ntxes=', txes.length, 'height=', block.height);
  col.findAndModify(
    {'hash': block.hash}, [],
    {$set: block},
    {upsert: true, new: true},
    function(err, b) {
      if(err) return callback(err);
      if(txes && txes.length > 0) {
	self.store.addTxes(txes, callback);
	//tmptxcol.insert(txes, callback);
      } else {
	callback();
      }
    });
};

BlockChain.prototype.updateBlock = function(block, callback) {
  var self = this;
  var col = this.store.dbconn.collection('block');
  var txcol = this.store.dbconn.collection('tx');
  var txes = block.txes;

  delete block.height;
  delete block._id;
  delete block.txes;

  async.series([
    function(c) {
      col.findAndModify(
	{hash:block.hash}, [],
	{$set: block},
	{upsert: false, 'new': false},
	function(err, b) {
	  if(err) return c(err);
	  if(!b) return c(new Skip('block not found'));
	  c();
	});
    },
    function(c) {
      txcol.remove({bhash: block.hash}, c);
    },
    function(c) {
      var txarr = [];
      txarr = txes.map(function(tx) { return tx.hash});
      txcol.find({hash: {$in: txarr}}).toArray(function(err, arr) {
	if(err) return c(err);
	arr.forEach(function(t) {
	  console.info('update tx', t.hash.toString('hex'));
	  });
	c();
      });
    },
    function(c) {
      console.info('tx cnt', txes.length);
      self.store.addTxes(txes, c);
    }
  ], function(err) {
    if(err instanceof Skip) {
      err = undefined;
    }
    callback(err);
  });
};

BlockChain.prototype.storeTipBlock = function(b, allowReorgs, cb) {
  b.isMain = true;

  if (typeof allowReorgs === 'function') {
    cb = allowReorgs;
    allowReorgs = true;
  }
  if (!b) return cb();

  var self = this;
  var oldTip, oldNext, needReorg = false;
  var newPrev;
  var noBlock = false;
  var start = new Date();
  var sumtime = 0;
  var tss = [];
  function pushts(label) {
    var t = (new Date() - start)/1000;
    sumtime += t;
    tss.push(label + '-' + t);
    start = new Date();
  }

  async.series([
    function(c) {
      self.store.getBlock(b.hash, function(err, val) {
	pushts('getblock');
	return c(err ||
		 (val? new Skip('Ignore already existing block:' + b.hash.toString('hex')): null));
      });
    },
    function(c) {
      self.store.dbconn.collection('block').count(function(err, n) {
	pushts('getcount');
	if(err) return c(err);
	noBlock = n <= 0;
	c();
      });
    },
    function(c) {
      if(!noBlock) return c();
      var genesisBlock = helper.clone(bitcore.networks[self.netname].genesisBlock);
      if(b.hash.toString('hex') == helper.reverseBuffer(genesisBlock.hash).toString('hex')) {
	self.insertBlock(b, function(err) {
	  pushts('insertblock');
	  if(err) return c(err);
	  self.store.setTipBlock(b, function(err) {
	    pushts('settip');
	    c(err || new Skip('Genesis block'));
	  });
	});
      } else c();
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.getBlock(b.prev_hash, function(err, val) {
	pushts('getprevblock');
	if(err) return c(err);
        if(!val) {
	  return c(new Skip('Ignore block due to non existing prev: ' + b.hash.toString('hex')));
	}
	newPrev = val;
	b.height = newPrev.height + 1;
	self.store.stats.maxHeight = b.height;
	self.store.stats.tipTimestamp = b.timestamp;
	c();
      });
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.getTipBlock(function(err, val) {
	pushts('gettipblock');
	if(err) return c(err);
        oldTip = val;
        if (oldTip && newPrev.hash.toString('hex') !== oldTip.hash.toString('hex')) needReorg = true;
        return c();
      });
    },
    function(c) {
      if (!needReorg) return c();
      self.store.getBlock(newPrev.next_hash, function(err, val) {
	pushts('getblockprevnext');
        if (err) return c(err);
        oldNext = val;
        return c();
      });
    },
    function(c) {
      self.insertBlock(b, function(err) {
	pushts('insertblock');
	c(err);
      });
    },
    function(c) {
      if (!needReorg) return c();
      console.log('NEW TIP: %s NEED REORG (old tip: %s)', b.hash.toString('hex'), oldTip.hash.toString('hex'));
      self.processReorg(oldTip, oldNext, newPrev, function(err) {
	pushts('processReorg');
	c(err);
      });
    },
    function(c) {
      if (!allowReorgs) return c();
      self.store.setTipBlock(b, function(err) {
	pushts('settipblock');
	c(err);
      });
    },
    function(c) {
      newPrev.next_hash = b.hash;
      self.store.saveBlock(newPrev, function(err) {
	pushts('saveblock');
	c();
      });
    }
  ],
	       function(err) {
		 if(sumtime > 10.0) {
		   console.info('storetipblock using', tss);
		 }
		 if(err && err instanceof Skip) {
		   //console.info('SKIP', err.message);
		   err = null;
		 }
		 return cb(err);
	       });
};

BlockChain.prototype.processReorg = function(oldTip, oldNext, newPrev, cb) {
  var self = this;
  var orphanizeFrom;

  async.series([
    function(c) {
      if(!newPrev.isMain) return c();
      orphanizeFrom = oldNext;
      console.log('# Reorg Case 1)');
      c();
    },
    function(c) {
      if (orphanizeFrom) return c();

      console.log('# Reorg Case 2)');
      self.setBlocksUnOrphan(newPrev, function(err, yBlock, newYBlockNext, arr) {
        if (err) return c(err);
	self.store.getBlock(yBlock.next_hash, function(err, yBlockNext) {
          orphanizeFrom = yBlockNext;
	  yBlock.next_hash = newYBlockNext.hash;
	  self.store.saveBlock(yBlock, c);
        });
      });
    },
    function(c) {
      if (!orphanizeFrom) return c();
      self.setBranchOrphan(orphanizeFrom, function(err, arr) {
	return c(err);
      });
    }
  ], cb);
};

BlockChain.prototype.setBranchOrphan = function(fromBlock, cb) {
  var self = this;
  var pblock = fromBlock;
  var blockActions = [];

  async.whilst(
    function() {
      return pblock;
    },
    function(c) {
      console.info('set orphan', pblock.hash.toString('hex'));
      self.setBlockMain(pblock, false, function(err, changed) {
        if (err) return c(err);
	if(changed)
	  blockActions.push(pblock.hash);
        self.store.getBlock(pblock.next_hash, function(err, val) {
	  if(err) return c(err);
	  pblock = val;
          return c(err);
        });
      });
    }, function(err) {
      cb(err, blockActions);
    });
};

BlockChain.prototype.setBlockMain = function(block, isMain, callback) {
  var self = this;
  var changed = isMain !== block.isMain;
  block.isMain = isMain;
  this.store.saveBlock(block, function(err) {
    if(err) return callback(err);
    callback(err, changed);
  });
};

BlockChain.prototype.setBlocksUnOrphan = function(fromBlock, cb) {
  var self = this;
  var currBlock = fromBlock;
  var lastBlock = fromBlock;
  var isMain;
  var blockActions = [];

  async.doWhilst(
    function(c) {
      self.setBlockMain(currBlock, true, function(err, changed) {
        if (err) return c(err);
	if(changed)
	  blockActions.push(currBlock.hash);
	self.store.getBlock(currBlock.prev_hash, function(err, val) {
          if (err) return c(err);
	  lastBlock = currBlock;
          currBlock = val;
          isMain = currBlock.isMain;
          return c();
        });
      });
    },
    function() {
      return currBlock && !isMain;
    },
    function(err) {
      //console.log('\tFound yBlock:', currBlock);
      return cb(err, currBlock, lastBlock, blockActions);
    }
  );
};

BlockChain.prototype.stop = function(cb) {
  var self = this;
  this.blockQueue.task({timestamp: 0,
			hexHash: ''},
		       function(c){
			 self.blockQueue.closed = true;
			 cb();
			 c();
		       });
};

BlockChain.prototype.enqueueBlock = function(blockContext) {
  var self = this;
  var blockObj = blockContext.blockObj;
  return this.blockQueue.task({timestamp:blockObj.timestamp,
			       hexHash:blockObj.hash.toString('hex')},
			      function(c) {
				self.storeTipBlock(blockObj, true, function(err) {
				  if(err) {
				    console.error(err);
				  }
				  c();
				});    
			      }, {hash: blockObj.hash.toString('hex')});
};

BlockChain.prototype.sync = function() {
  // Do nothing
};

module.exports = BlockChain;
