
print(new Date(), 'begin update tx');
db.tx.find({}, {hash: 1, vin:1, vout:1}).forEach(function(tx) {
  tx.vin.forEach(function(input) {
    if(input.hash) {
      var v = {};
      v['spt.' + input.n] = 1;
      db.txinfo.update({_id: input.hash}, {$set: v}, {upsert: true});
    }
  });
  var addrs = [];
  tx.vout.forEach(function(output) {
    if(output.addrs) {
      addrs.push(output.addrs.join(','));
    } else {
      addrs.push('');
    }
  });
  db.txinfo.update({_id: tx.hash}, {$set: {addrs: addrs}}, {upsert: true});
});

print(new Date(), 'end update tx');
