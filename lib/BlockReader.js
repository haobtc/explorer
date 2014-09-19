'use strict';
var bitcore = require('bitcore-multicoin'),
     Block    = bitcore.Block,
     networks = bitcore.networks,
     Parser   = bitcore.BinaryParser,
     fs       = require('fs'),
     Buffer   = bitcore.Buffer,
     glob     = require('glob'),
     async    = require('async');

function BlockReader(dataDir, netname) {
  var path = dataDir + '/blocks/blk*.dat';

  this.dataDir = dataDir;
  this.files   = glob.sync(path);
  this.nfiles  = this.files.length;
  this.errorCount = 0;

  if (this.nfiles === 0)
    throw new Error('Could not find block files at: ' + path);

  this.currentFileIndex = 0;
  this.initialPosition = 0;
  this.isCurrentRead    = false;
  this.currentBuffer    = null;
  this.currentParser    = null;
  this.network = networks[netname];
  this.magic   = this.network.magic.toString('hex');
}

BlockReader.prototype.currentFile = function() {
  return this.files[this.currentFileIndex];
};


BlockReader.prototype.nextFile = function() {
  if (this.currentFileIndex < 0) return false;

  var ret  = true;

  this.isCurrentRead = false;
  this.currentBuffer = null;
  this.currentParser = null;
  this.errorCount = 0;

  if (this.currentFileIndex < this.nfiles - 1) {
    this.currentFileIndex++;
  }
  else {
    this.currentFileIndex=-1;
    ret = false;
  }
  return ret;
};


BlockReader.prototype.rewind = function(fileIndex, pos) {
  if(this.currentFileIndex  == fileIndex &&
     pos == this.pos()) {
    return;
  }    

  this.currentFileIndex = fileIndex;
  this.initialPosition = pos;
  this.isCurrentRead = false;
  this.currentBuffer = null;
  this.currentParser = null;
};

BlockReader.prototype.readBlocks = function(n, callback) {
  var self = this;
  var times = 0;
  var cblock;
  var blocks = [];
  async.doWhilst(function(c) {
    var pos = self.pos();
    var fileIndex = self.currentFileIndex;
    self.getNextBlock(function(err, b) {
      if(err) return c(err);
      cblock = b;
      times++;
      if(b) {
	blocks.push({block: b, fileIndex:fileIndex, pos: pos});
      }
      c();
    });
  }, function() {
    return times < n && cblock;
  }, function(err) {
    callback(err, blocks);
  });
};

BlockReader.prototype.pos = function() {
  if(this.currentParser) {
    return this.currentParser.pos;
  } else {
    return this.initialPosition;
  }
}

BlockReader.prototype.readCurrentFileSync = function() {
  if (this.currentFileIndex < 0 || this.isCurrentRead) return;

  this.isCurrentRead = true;

  var fname = this.currentFile();
  if (!fname) return;


  var stats = fs.statSync(fname);

  var size = stats.size;

  console.log('Reading Blockfile %s [%d MB]',
            fname, parseInt(size/1024/1024));

  var fd = fs.openSync(fname, 'r');

  var buffer = new Buffer(size);

  fs.readSync(fd, buffer, 0, size, 0);

  this.currentBuffer = buffer;
  this.currentParser = new Parser(buffer);
  this.currentParser.pos = this.initialPosition;
  this.initialPosition = 0;
};

BlockReader.prototype._getMagic = function() {
  if (!this.currentParser) 
    return null;

  var byte0 = this.currentParser ? this.currentParser.buffer(1).toString('hex') : null;



  // Grab 3 bytes from block without removing them
  var p = this.currentParser.pos;
  var bytes123 = this.currentParser.subject.toString('hex',p,p+3);
  var magic = byte0 + bytes123;


  if (magic !=='00000000' && magic !== this.magic) {
    console.info('magic', magic, this.errorCount);
    if(this.errorCount++ > 4)
      throw new Error('CRITICAL ERROR: Magic number mismatch: ' +
                          magic + '!=' + this.magic);
    magic=null;
  }
  
  if (magic==='00000000') 
    magic =null;

  return magic;
};

BlockReader.prototype.getNextBlock = function(cb) {
  var b;
  var magic;
  var isFinished = 0;

  while(!magic && !isFinished)  {
    this.readCurrentFileSync();
    magic= this._getMagic();

    if (!this.currentParser || this.currentParser.eof() ) {

      if (this.nextFile()) {
        console.log('Moving forward to file:' + this.currentFile() );
        magic = null;
      } else {
        console.log('Finished all files');
        isFinished = 1;
      }
    }
  }
  if (isFinished)
    return cb();

  // Remove 3 bytes from magic and spacer
  this.currentParser.buffer(3+4);

  b = new Block();
  b.parse(this.currentParser);
  b.getHash();
  //this.errorCount=0;
  return cb(null,b);
};

module.exports = BlockReader;

