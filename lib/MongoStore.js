var mongodb = require('mongodb');

module.exports.stores = {};

module.exports.initialize = function(netnames, callback) {
  netnames.forEach(function(netname) {
    var store = Store(netname);
    module.exports.stores[netname] = store;
    store.connect(function(err, conn) {
      if(typeof callback=='function') callback(err, netname);
    });
  });
};

function Store(network) {
  var store = {};
  var conn;
  var pendingBlocks = {};

  store.connect = function(callback) {
    var url = "mongodb://localhost:27017/blocks_" + network;
    mongodb.MongoClient.connect(url, function(err, aConn) {
      if(err) {
        console.error(err);
	if(typeof callback == 'function') callback(err);
        return;
      }
      conn = aConn;

      function nf(err) {if(err){console.error(err);}}

      var blockCol = conn.collection('block');
      blockCol.ensureIndex({'hash': 1}, {unique: 1}, nf);
      blockCol.ensureIndex({'height': -1}, nf);

      var txCol = conn.collection('tx');
      txCol.ensureIndex({'vin.hash': 1, 'vin.n': 1}, {unique: 1, sparse: 1}, nf);
      txCol.ensureIndex({'vout.addrs': 1}, {}, nf);

      if(typeof callback == 'function') callback(conn);
    });
  };

  store.conn = function() {
    return conn;
  };

  store.getLatestBlock = function(callback) {
    var col = conn.collection('block');
    col.find().sort({height: -1}).limit(1).toArray(function(err, results) {
      if(err) {
	return callback(err);
      }
      var block = results[0];
      if(block) {
	block.hash = toBuffer(block.hash);
	block.prev_hash = toBuffer(block.prev_hash);
      }
      callback(err, block);
    });
  };

  
  function toBuffer(hash) {
    if(hash instanceof mongodb.Binary) {
      return hash.buffer;
    } else if(hash instanceof Buffer) {
      return hash;
    } else {
      return new Buffer(hash, 'hex');
    }
  }

  store.getBlock = function(hash, callback) {
    var col = conn.collection('block');
    col.find({hash: hash}).toArray(function(err, results) {
      if(err) {
	return callback(err);
      }
      var block = results[0];
      if(block) {
	block.hash = toBuffer(block.hash);
	block.prev_hash = toBuffer(block.prev_hash);
      }
      callback(undefined, block);
    });
  }

  function removeOrphanBlocks(block, callback) {
    var col = conn.collection('block');
    col.find({height: {$gt: block.height}}).toArray(function(err, results) {
      if(err) {
	return callback(err);
      }
      results.forEach(function(sb) {
	delete sb._id;
	pendingBlocks[sb.hash.buffer.toString('hex')] = block;
      });

      if(results.length > 0) {
	col.remove({height: {$gt: block.height}}, callback);
      } else {
	callback();
      }
    });
  }

  function saveBlock(block, callback) {
    var col = conn.collection('block');
    if(typeof callback != 'function') {
      callback = function(err) {if(err){console.error(err)}};
    }
    delete block._id;
    var txes = block.txes;
    delete block.txes;
    console.info('save block', block.hash.toString('hex'));
    col.findAndModify({'hash': block.hash}, [],
		     {$set: block},
		      {upsert: true},
		      function(err) {
			if(err) {
			  return callback(err);
			}
			if(txes && txes.length > 0) {
			  var txcol = conn.collection('tx');
			  txcol.insert(txes, function(err) {
			    if(err) {
			      return callback(err);
			    }
			    findPendings(block, callback);
			  });
			} else {
			  findPendings(block, callback);
			}
		      });
  }

  function findPendings(block, callback) {
    var col = conn.collection('block');
    for(var pHashString in pendingBlocks) {
      var pBlock = pendingBlocks[pHashString];
      if(pBlock && pHashString == block.hash.toString('hex')) {
	delete pendingBlocks[pHashString];
	pBlock.height = block.height + 1;
	return saveBlock(pBlock, callback);
      }
    }
    callback();
  }

  var pushingBlockFifo = [];
  var pushingBlock = false;
  var lastPushDate = new Date();
  store.isPushingBlock = function() {
    return ((pushingBlock || pushingBlockFifo.length > 0) &&
	    new Date() - lastPushDate >= 50000);
  };
  store.pipedPushBlock = function(block, callback) {
    pushingBlockFifo.unshift({
      block: block,
      callback: callback
    });
    function pickup() {
      if(pushingBlock) {
	return;
      }
      var st = pushingBlockFifo.pop();
      if(st) {
	pushingBlock = true;
	store.pushBlock(st.block, function(err) {
	  if(typeof st.callback == 'function') {
	    st.callback(err);
	  }
	  pushingBlock = false;
	  lastPushDate = new Date();
	  setTimeout(pickup, 0);
	});
      }
    }
    pickup();
  };

  store.pushBlock = function(block, callback) {
    console.log('** Block Received **', block.hash.toString('hex'), block.prev_hash.toString('hex'));
    var col = conn.collection('block');
    store.getBlock(block.hash, function(err, aBlock) {
      if(err) {
	return callback(err);
      }
      if(aBlock) {
	// Already have one, return;
	return callback();
      }

      col.findOne(function(err, firstBlock) {
	if(err) {
	  return callback(err);
	}
	if(firstBlock) {
	  // Already have at least one block
	  store.getBlock(block.prev_hash, function(err, prevBlock) {
	    if(err) {
	      return callback(err);
	    }
	    if(prevBlock) {
	      removeOrphanBlocks(prevBlock, function(err) {
		if(err) {
		  return callback(err);
		}
		block.height = prevBlock.height + 1;
		saveBlock(block, callback);
	      });
	    } else {
	      pendingBlocks[block.hash.toString('hex')] = block;	    
	      return callback();
	    }
	  });
	} else {
	  saveBlock(block, callback);
	}
      });
    });
  };
  return store;
}



