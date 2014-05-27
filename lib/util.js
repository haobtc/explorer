module.exports.reverseBuffer = function(hash) {
  var reversed = new Buffer(hash.length);
  hash.copy(reversed);
  buffertools.reverse(reversed);
  return reversed;
}
