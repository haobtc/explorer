'use strict';

function fillInputs(tx) {
  var updateSet = {};
  var changed = false;
  tx.vin.forEach(function(input, index) {
    if(!input.addrs && input.hash) {
      var inputTx = db.tx.findOne({hash: input.hash});
      if(inputTx) {
	input.addrs = inputTx.vout[input.n].addrs;
	input.v = inputTx.vout[input.n].v;
	updateSet['vin.' + index + '.addrs'] = input.addrs;
	updateSet['vin.' + index + '.v'] = input.v;
	changed = true;
      } else {
	var message = 'Cannot find input of ' + input.hash.hex() + '#' + input.n + ' by ' + tx.hash.hex() + '.vin.' + index;
	db.errormsg.insert({message: message});
      }
    }
  });
  if(changed) {
    db.tx.update({hash: tx.hash}, {$set: updateSet});
  }
}

function updateInputSpt(tx) {
  tx.vin.forEach(function(input) {
    if(input.hash) {
      var updateSet = {};
      updateSet['vout.' + input.n + '.spt'] = 1;
      db.tx.update({hash: input.hash}, {$set: updateSet});
    }
  });
}

function updateAddress(tx) {
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
      db.address.update({_id: addr}, {$inc: {'bal': -amount}}, {upsert: true});
    }
  });
  tx.vout.forEach(function(output) {
    if(output.addrs) {
      var addr = output.addrs.join(',');
      loadAddr(addr);
      var amount = NumberLong(output.v);
      addrAmount[addr] += amount;
      db.address.update({_id: addr}, 
			{$inc: {'bal': amount, 'recv': amount}}, {upsert: true});
    }
  });

  for(var addr in addrAmount) {
    if(addrAmount.hasOwnProperty(addr)) {
      var a = db.address.findOne({_id: addr});
      var txs = (a&&a.txs)?a.txs:[];
      txs.push({t: tx.hash,v:addrAmount[addr]});
      while(txs.length > 50) {
	txs.shift();
      }
      db.address.update({_id: addr}, {$inc: {'c': 1}, $set: {txs: txs}});
    }
  }
}

function cursorNext() {
  db.runCommand('beginTransaction');

  // Get tx
  var v = db.var.findOne({key: 'txcursor'});
  var tx;
  if(!v) {
    tx = db.tx.findOne();
  } else {
    tx = db.tx.find({_id: {$gt: v.objid}}).sort({_id: 1}).limit(1)[0];
  }

  if(!tx) {
    db.runCommand('rollbackTransaction');
    sleep(1000);
    quit();
    return false;
  }
  //print(tx.hash.hex());
  // fill the input addrs and v of tx
  fillInputs(tx);

  updateInputSpt(tx);
  updateAddress(tx);  

  db.var.update({key: 'txcursor'},  {$set: {objid: tx._id}}, {upsert: true});
  db.runCommand('commitTransaction');
  return true;
}

for(var i=0; i<10000; i++) {
  if(i % 1000 == 0){
    print(i);
  }
  cursorNext();
}