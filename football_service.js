// --------------------------------------------------------

var createError = require('http-errors');
var express = require('express');
var http = require('http');
var app = express();
const match = require('./api/matches/matches.controller');

// --------------------------------------------------------


initExpress();


// --------------------------------------------------------

function initExpress() {
    
    app.use(express.json());
    app.use(express.urlencoded({ extended: false }));
    app.use('/matches', require('./api/matches/'));
    app.use('/commentaries', require('./api/commentaries/'));
    app.use('/standings', require('./api/standings/'));
    app.use('/player', require('./api/player/'));
    app.use('/team', require('./api/team/'));
    
    app.use(function(req, res, next) {
        next(createError(404));
    });
    
    if (app.get('env') === 'development') {
        app.use(function(err, req, res, next) {
            res.status(err.status || 500);
            res.json( {
            result : 0,
            message: err.message,
            error: err
            });
        });
    }      
    
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.json( {
            result : 0,
            message: err.message,
            error: err
        });
    });
    
    http.createServer(app).listen(4810, function(){
        console.log('Hifive football_service server listening on port ' + 4810);
    });
    
    module.exports = app;
}