
function f(cb) {
  cb(new Error('err'));
};

f(function(err, v) {
  if(err) console.log('1');
  else console.log('2');
});
