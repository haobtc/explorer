
db.block.ensureIndex({processed: 1}, {sparse: 1});

function processBlock(block) {
  var allTxes = [];
  var idx = 0;
  var txes = db.tx.find({bhash: block.hash}).sort({_id: 1});
  txes.forEach(function(tx) {
    tx.bhs = [block.hash];
    tx.bis = [idx];
    idx++;
    allTxes.push(tx);
    delete tx.bhash;
    db.tx.save(tx);
  });

  block.processed = true;
  block.cntTxes = allTxes.length;
  db.block.save(block);
}

for(var i=0; i<5000; i++) {

  var blocks = db.block.find({processed: {$exists: false}}).limit(100);
  print('t', i * 100, blocks.count());
  var hasBlock = false;
  blocks.forEach(function(block) {
    //print('block.hash', block.hash.hex(), block.isMain);
    hasBlock = true;
    if(block.isMain) {
      processBlock(block);
    } else {
      block.processed = true;
      db.block.save(block);
    }
  });
  if(!hasBlock) break;
}
