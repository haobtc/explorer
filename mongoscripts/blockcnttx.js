
db.block.find().sort({$natural: -1}).limit(12000).forEach(function(block) {
  if(!block.isMain) return;
  var cnt = db.tx.find({bhs: block.hash}).count();
  print('cnt', cnt);
  if(block.cntTxes != cnt){
    print(block.hash.hex(), block.cntTxes, cnt, block.height);
  }
});
