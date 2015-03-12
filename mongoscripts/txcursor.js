'use strict';

function TxCursor(url) {
  this.db = connect(url);
}

TxCursor.prototype.fillInputs = function(tx) {
  var self = this;
  var updateSet = {};
  var changed = false;
  tx.vin.forEach(function(input, index) {
    if(!input.addrs && input.hash) {
      var inputTx = self.db.tx.findOne({hash: input.hash});
      if(inputTx) {
	input.addrs = inputTx.vout[input.n].addrs;
	input.v = inputTx.vout[input.n].v;
	updateSet['vin.' + index + '.addrs'] = input.addrs;
	updateSet['vin.' + index + '.v'] = input.v;
	changed = true;
      } else {
	var message = 'Cannot find input of ' + input.hash.hex() + '#' + input.n + ' by ' + tx.hash.hex() + '.vin.' + index;
	self.db.errormsg.insert({message: message});
      }
    }
  });
  if(changed) {
    this.db.tx.update({hash: tx.hash}, {$set: updateSet});
  }
};

TxCursor.prototype.updateInputSpt = function(tx) {
  var self = this;
  tx.vin.forEach(function(input) {
    if(input.hash) {
      var updateSet = {};
      updateSet['vout.' + input.n + '.spt'] = 1;
      self.db.tx.update({hash: input.hash}, {$set: updateSet});
    }
  });
};

TxCursor.prototype.updateAddress = function(tx) {
  var self = this;
  var addrAmount = {};
  function loadAddr(addr) {
    if(!addrAmount[addr]) {
      addrAmount[addr] = NumberLong(0);
    }
  }

  tx.vin.forEach(function(input) {
    if(input.addrs) {
      var addr = input.addrs.join(',');
      loadAddr(addr);
      var amount = NumberLong(input.v);
      addrAmount[addr] -= amount;
      self.db.address.update({_id: addr}, {$inc: {'bal': -amount}}, {upsert: true});
    }
  });
  tx.vout.forEach(function(output) {
    if(output.addrs) {
      var addr = output.addrs.join(',');
      loadAddr(addr);
      var amount = NumberLong(output.v);
      addrAmount[addr] += amount;
      self.db.address.update({_id: addr}, 
			     {$inc: {'bal': amount, 'recv': amount}}, {upsert: true});
    }
  });

  for(var addr in addrAmount) {
    if(addrAmount.hasOwnProperty(addr)) {
      var a = self.db.address.findOne({_id: addr});
      var txs = (a&&a.txs)?a.txs:[];
      txs.push({t: tx.hash,v:addrAmount[addr]});
      while(txs.length > 50) {
	txs.shift();
      }
      self.db.address.update({_id: addr}, {$inc: {'c': 1}, $set: {txs: txs}});
    }
  }
};

TxCursor.prototype.next = function() {
  this.db.runCommand('beginTransaction');

  // Get tx
  var v = this.db.var.findOne({key: 'txcursor'});
  var tx;
  if(!v) {
    tx = this.db.tx.findOne();
  } else {
    tx = this.db.tx.find({_id: {$gt: v.objid}}).sort({_id: 1}).limit(1)[0];
  }

  if(!tx) {
    this.db.runCommand('rollbackTransaction');
    return false;
  }
  this.fillInputs(tx);

  this.updateInputSpt(tx);
  this.updateAddress(tx);  

  this.db.var.update({key: 'txcursor'},  {$set: {objid: tx._id}}, {upsert: true});
  this.db.runCommand('commitTransaction');
  return true;
};

// TxCursor.prototype.blockJob = function() {
//   this.db.runCommand('beginTransaction');
//   var v = this.db.var.findOne({key: 'blockCleanup'});
  
//   this.db.runCommand('commitTransaction');
// };

var processed = 0;

['bitcoin', 'dogecoin', 'litecoin', 'darkcoin'].forEach(function(network) {
  var txCursor = new TxCursor('localhost:27017/blocks_' + network);
  for(var i=0; i<10000; i++) {
    var r = txCursor.next();
    if(!r) break;
    processed++;
  }
});

if(processed > 0) {
  sleep(1.0);
}

