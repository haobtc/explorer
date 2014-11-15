
db.block.ensureIndex({processed: 1}, {sparse: 1});

function processBlock(block) {
  db.tx.find({bhs: block.hash}, {_id: 1, vin: 1}).forEach(function(tx) {
    tx.vin.forEach(function(input) {
      if(input.hash) {
	var q = {};
	var inc = {};
	inc['vout.' + input.n + '.spt'] =  1;
	q.$set = inc;
	//print(JSON.stringify(q));
	db.tx.update({'hash': input.hash}, q);
      }
    });
  });
  block.processed = true;
  db.block.save(block);
}

for(var i=0; i<5000; i++) {
  print('t', i * 100);
  var blocks = db.block.find({processed: {$exists: false}}).limit(100);
  var hasBlock = false;
  blocks.forEach(function(block) {
    hasBlock = true;
    if(block.isMain){
      processBlock(block);
    } else {
      db.block.update({hash: block.hash}, {$set: {processed: true}})
    }
  }); 
  if(!hasBlock) {
    break;
  }
}



