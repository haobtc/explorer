var block_height = 372463;

db.block.find({height: {$gt: block_height}}).forEach(function(b) {
  db.tx.remove({bhash: b.hash});
});
print('removed adv txes');

db.block.remove({height: {$gt: block_height}});

var maxb = db.block.find({isMain: true}).sort({height: -1}).limit(1)[0];
print(maxb);
if(maxb) {
  db.var.update({key: 'tip'}, {$set: {blockHash: maxb.hash}});
}
db.archive.remove();
db.mempool.remove();

