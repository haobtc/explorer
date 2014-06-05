// Defer object
Array.prototype.pushArguments = function (args) {
    for(var i=0; i<args.length; i++) {
        this.push(args[i]);
    }
};

function Defer() {
    var defer = {};
    defer.isOK = false;
    defer.callbacks = [];
    defer.args = [];
    defer.avail = function() {
        if (defer.isOK) {
            return;
        }
        defer.args.pushArguments(arguments);
        defer.isOK = true;
        defer.callbacks.forEach(function (cb) {
            cb.apply(null, defer.args);
        });
        defer.callbacks = [];
    };

    defer.then = function(fn) {
        if (this.isOK) {
            fn.apply(null, defer.args);
        } else {
            defer.callbacks.push(fn);
        }
    };

    defer.wait = function(childDefers, opts) {
	opts = opts || {};

        var counter = childDefers.length;
        var results = [];
        for (var i=0; i<counter; i++) {
            results.push(null);
        }
        
        if (counter > 0) {
            childDefers.forEach(function(c, index) {
                c.then(function() {
                    if (arguments.length > 0) {
                        var arr = [];
                        arr.pushArguments(arguments);
                        results[index] = arr;
                    }
                    counter--;
                    if (counter == 0) {
			if(opts.flatten) {
			    var flatResults = [];
			    results.forEach(function(rs) {
				if(rs) {
				    rs.forEach(function(e) {
					flatResults.push(e);
				    });
				}
			    });
			    defer.avail.call(null, flatResults);
			} else {
                            defer.avail.call(null, results);
			}
                    }
                });
            });
        }
    };
    return defer;
}

module.exports = Defer;
