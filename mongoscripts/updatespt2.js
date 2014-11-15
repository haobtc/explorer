
function txMapper() {
  var self = this;
  this.vin.forEach(function(input) {
    if(input.hash) {
      var v = [];
      v[input.n] = 1;
      emit(input.hash, v);
    }    
  });
}

function txReducer(key, values) {
  var obj = {};
  values.forEach(function(v) {
    for(var k in v) {
      obj[k] = 1;
    }
  });
  return obj;
}

print(new Date(), 'map reduce to gen txspt');
var r = db.runCommand({
  mapreduce: 'tx',
  map: txMapper,
  reduce: txReducer,
  out: 'txspt',
  sort: {hash: 1}
});
print(new Date(), 'gen txspt done');
print(JSON.stringify(r));

print(new Date(), 'ensure index txspt._id');
db.txspt.ensureIndex({_id: 1});

print(new Date(), 'update tx objid');
db.tx.find().forEach(function(tx) {
  db.txspt.update({_id: tx.hash}, {$set: {objid: tx._id}});
});
print(new Date(), 'ensure index objid');
db.txspt.ensureIndex({objid: 1});
print(new Date(), 'update spt');
db.txspt.find().sort({objid: 1}).forEach(function(txspt){
  if(!txspt.objid) {
    return;
  }
  var v = {};
  for(var n in txspt.value) {
    v['vout.' + n + '.spt'] = 1;
  }
  db.tx.update({_id: txspt.objid}, {$set: v});
});
print(new Date(), 'update spt done');
