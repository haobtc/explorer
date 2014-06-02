var express = require('express');
var app = express();
var bodyParser = require('body-parser');

app.use(bodyParser());
app.use('/explorer/', express.static('public'));
app.use(function(err, req, res, next){
    console.error(err.stack);
    res.send({error: true});
});

module.exports.httpServer = app;
