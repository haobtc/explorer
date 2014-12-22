
db.block.find().sort({$natural: -1}).skip(0).limit(10000).forEach(function(block) {
  if(!block.isMain) return;
  var cnt = db.tx.find({bhs: block.hash}).count();
  print('cnt', cnt);
  if(block.cntTxes != cnt){
    print(block.hash.hex());
  }
});
