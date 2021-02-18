const schedule = require('node-schedule')
const db = require('./db')
const db2 = require('./db2')
const request = require('request');
const converter_standings = require('./converter_standings')
const converter_matches = require('./converter_matches')
const converter_commentaries = require('./converter_commentaries')
const converter_players = require('./converter_players')
const converter_teams = require('./converter_teams')
// const collect_best_contents = require('./collect_best_contents')
const caster = require('./caster')
const mqtt_sender = require('./mqtt_sender')

// --------------------------------------------------------

var createError = require('http-errors');
var express = require('express');
var http = require('http');
var app = express();
const match = require('./api/matches/matches.controller');

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
    
        // const server = http.createServer(app).listen(7770, function(){
    
        //     console.log('Caster API server listening on port ' + 7770);
        
        // });  

        const server = http.createServer((req, res) => {
            res.statusCode = 200;
            res.setHeader('Content-Type', 'text/plain');
            res.end('Hello World\n');
          });

        server.listen(4810, function() {

            var host2 = server.address().address;
            var port2 = server.address().port;



            console.log(`Server running at http://${host2}:${port2}/`);
          });
    
        module.exports = app;
}

//initExpress();

async function fnMain() {
    
    console.log('fnMain-start');

    //converter_players.converterPlayerToId(82742)
    converter_matches.converterMatchTodate('20180701', '20181231');
    //converter_teams.convertTeamsNotExists();

    // var [rows, fields] = await fnGetQuery2();

    // for(var i =0; i < rows.length; i++) {
        
    //     var lineup = rows[i];
    //     console.log('player name : ' + lineup.player_name)

    // }


    //converter_matches.converterMatchTodate('20180601', '20191231');

    // var list = await fnGetQuery();

    // for(var i =0; i < list.length; i++) {

    //     var lineup = list[i];
    //     //mqtt_sender.sendMQTT('match', lineup);
    //     console.log('player name : ' + list[i].player_name)

    //     const baseURL = "http://127.0.0.1:5550/reaction/"
    //     let url = baseURL

    //     request.get(url , function (error, response, body) {

    //         if ( error ) {
    //             console.log('request error : ' + error)
    //         }
    //       });  
    // }

    console.log('fnMain-end');
    
}

async function fnGetQuery2() {
    
    let query = 
    `
    select *
    from   lineup
    where  match_id   = 252083
    and    team_fa_id = 12303
    ;
    `
    let parameter = [];

    var result = await db2.getQuery(query, parameter);

    return result;
}

function fnGetQuery() {
    
    let query = 
    `
    select *
    from   lineup
    where  match_id   = 252083
    and    team_fa_id = 12303
    ;
    `
    let parameter = [];

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log("asyncGetPlayersToDate 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

///fnMain();

//converter_commentaries.converterCommentaries();
//converter_players.convertPlayersLineupNotExists();
//converter_teams.convertAllTeams();
// converter_standings.converterStandingToId(1056);
//converter_standings.converterStandings();
//converter_commentaries.converterUpCommingCommentaries()
//converter_players.convertPlayersLineupNotExists()
//converter_players.convertNationalPlayers();
//converter_players.convertTeamSquadToId(2011);
//converter_players.converterPlayerToId(82742)
//convertNationalPlayers();
//converter_matches.converterMatchTodate(1535, '20180501', '20180730');
//converter_matches.converterMatchTodate('20190101', '20191231');
//converter_matches.converterMatchToId(2354219);
//converter_matches.converterMatchTodate('20180401', '20180430');



console.log('AAAAA');

//result count 
// let message = {}
// message.push_alert = true
// message.message = 'Top 인기글이 업데이트 되었습니다'

// let messageBody = {}
// messageBody.for_apns = {}
// messageBody.for_apns = message
// messageBody.for_gcm  = message

// let pushMessage = JSON.stringify(messageBody)

//converter_standings.converterStandings();
//converter_matches.converterMatches();
//converter_commentaries.converterLiveCommentaries();
//converter_matches.converterMatches();
//converter_players.convertPlayersLineupNotExists();

//console.log(pushMessage);