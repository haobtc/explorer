var Queue = require('../lib/queue');
var async = require('async');

var q1 = new Queue({timeout:60000, limit:1810});
var q2 = new Queue({timeout:60000, limit:1810});

q1.task({key:1},
        function(c) {
          setTimeout(function() {
            console.log('task1 executed');
            c();
          }, 10000);
        }, {info:1});

q2.task({key:2},
        function(c) {
          setTimeout(function() {
            console.log('task2 executed');
            c();
          }, 20000);
        }, {info:1});

function stop(cb) {
  async.series([
    function(cc) {
      q1.task({key:0},
              function(c) {
                console.log('task0 executed');
                q1.closed = true;
                cc();
                c();
              }, {info:0});
    },
    function(cc) {
      q2.task({key:00},
              function(c) {
                console.log('task00 executed');
                q2.closed = true;
                cc();
                c();
              }, {info:'00'});
    }
  ], function(err) {
    if(err) console.error(err);
    cb();
  });
}

function stopNode() {
  console.log('stopNode start');
  stop(function() {
    console.log('stopped.\n');
    process.exit();
  });
}

process.on('SIGINT', stopNode);
process.on('SIGTERM', stopNode);
