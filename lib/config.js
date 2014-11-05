var path = require('path');
var multijson = require('multijson-config');

var fs = require('fs');


var config = multijson.parseJSONFiles(path.resolve(__dirname, '..', 'config.default.json'),
				      path.resolve(__dirname, '..', 'config.local.json'))

module.exports = config;
