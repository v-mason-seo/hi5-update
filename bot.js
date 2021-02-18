//https://www.npmjs.com/package/node-schedule 설명서
// 30 */1 * * * 한시간마다 30분에 실행

// *    *    *    *    *    *
// ┬    ┬    ┬    ┬    ┬    ┬
// │    │    │    │    │    |
// │    │    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
// │    │    │    │    └───── month (1 - 12)
// │    │    │    └────────── day of month (1 - 31)
// │    │    └─────────────── hour (0 - 23)
// │    └──────────────────── minute (0 - 59)
// └───────────────────────── second (0 - 59, OPTIONAL)


const schedule = require('node-schedule')
const db = require('./db')

const converter_standings = require('./converter_standings')
const converter_matches = require('./converter_matches')
const converter_commentaries = require('./converter_commentaries')
const converter_players = require('./converter_players')
const converter_teams = require('./converter_teams')
const collect_best_contents = require('./collect_best_contents')
const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');

// --------------------------------------------------------

var createError = require('http-errors');
var express = require('express');
var http = require('http');
var app = express();
const match = require('./api/matches/matches.controller');

// --------------------------------------------------------

/**
 * todo
 * 
 * 1.친선전 같은 경기는 코멘터리 정보 업데이트 간격을 수정해야 함.
 */

function main(){

    console.log('matin start');

    //converter_matches.converterMatchToId(2213165);
    //converter_matches.converterEndMatches();
    //converter_commentaries.converterCommentariesToId(2326166);
    //converter_commentaries.converterCommentaries();
    //converter_players.converterPlayerToId(17948);
    var query = 
    // `
    // select *
    // from   players
    // where  updated < '20180902'
    // order by updated desc
    // `;
    `
    select *
    from   players p
    where  1=1
    #and    p.player_id = 1099
    and    exists ( select 'x'
                    from    teams t
                    where  1=1
                    and     p.teamid = t.team_fa_id
                    and     t.is_national = 1
               );    
    `;

    //converter_players.converterPlayerbySql(query);
    //converter_players.convertPlayersToDate('20180701', '20180730');
    //collect_best_contents.collectBestContents('D');
    //collect_best_contents.collectForceBestContents('D', '2017-12-28 00:00:00', '2017-12-28 23:59:59');
    //converter_teams.convertTeamToId(20118);
    //converter_teams.convertTeamToId(6461);
    //converter_teams.convertAllTeams()
    //converter_teams.updatePlayerTeamSquad();
    //converter_players.convertPlayersNotExists();
    //converter_matches.converterMatchToId(2279928);
    //converter_matches.converterMatchTodate('20180916', '20180916');
    //converter_commentaries.converterLiveCommentaries();
    //converter_commentaries.converterCommentariesToId(774056);
    //converter_teams.updatePlayerTeamSquad();

    //converter_players.convertPlayersLineupNotExists();

        // var fromdate = new Date();
        // var todate = new Date();
        // fromdate.setHours(fromdate.getHours() - 168);
        // todate.setHours(todate.getHours() + 168);

        // var from = moment.utc(fromdate).format('YYYYMMDD');
        // var to = moment.utc(todate).format('YYYYMMDD');

        // console.log('from : ' + from + ', to : ' + to);

        // converter_matches.converterMatchTodate(from, to);

    /**
     * 리그 순위표
     * 코드 : CS
     */
    var standingUpdater = schedule.scheduleJob('1 */1 * * *', function() {
        converter_standings.converterStandings();
    });

    /**
     * 어제, 오늘, 내일 매치 정보 입력 및 업데이트
     * 시간은 런던시간 기준
     */
    // 2018-09-18 30 */12 -> 30 */2 변경
    var futureMatchUpdater = schedule.scheduleJob('30 */2 * * *', function() {

        var fromdate = new Date();
        var todate = new Date();
        fromdate.setHours(fromdate.getHours() - 168);
        todate.setHours(todate.getHours() + 168);

        var from = moment.utc(fromdate).format('YYYYMMDD');
        var to = moment.utc(todate).format('YYYYMMDD');

        converter_matches.converterMatchTodate(from, to);
    }); 

    /**
     * 조회조건 : 1. 경기시작시간 between -2 day and + 12 hour
     *          2. matches.timer is null ( 라이브 경기는 제외함. 중복으로 조회할 수 있기 때문에)
     * 라인업, 경기 코멘트리, 실시간 정보 등 업데이트
     * 코드 : CCMD
     */
    var commentariesUpdater = schedule.scheduleJob('21 */1 * * *', function() {
        converter_commentaries.converterCommentaries();
    });


    /**
     * 경기시작시간 1시간 전부터는 15분 단위로 업데이트 한다.
     * 라인업, 경기 코멘트리, 실시간 정보 등 업데이트
     */
    var commentariesUpdater2 = schedule.scheduleJob('*/15 * * * *', function() {
        converter_commentaries.converterUpCommingCommentaries();
    });

    /**
     * 막 끝난 매치 코멘터리 업데이트
     *  - status = 'FT'
     *  - 경기시작 시간 + 4시간 까지
     */
    var commentariesUpdater3 = schedule.scheduleJob('30 */1 * * *', function() {
        converter_commentaries.converterEndMatchCommentaries();
    });

    /**
     * 매치정보 업데이트
     * football api 호출할 때 파라미터를 넘기지 않으면 킥오프 기준 4시간 전부터 1시간후의 데이터를 가져온다.
     * 코드 : CM
     */
    var matchUpdater = schedule.scheduleJob('*/2 * * * *', function() {
        converter_matches.converterMatches();
    });

    /**
     * 라이브 매치
     * 조회조건 : 1. 경기시작시간 between -1 minute ~ + 160 minute
     *           2. matches.timer is null ( 라이브 경기는 제외함. 중복으로 조회할 수 있기 때문에)
     * 테스트로 2분마다 -> 10분마다(리밋걸림, 테스트) -> 3분 -> 5분
     */
    var commentariesLiveUpdater = schedule.scheduleJob('*/5 * * * *', function() {
        converter_commentaries.converterLiveCommentaries();
    });

    /**
     * 매치정보는 있지만 라인업 정보가 없는 매치
     * 사용안함 - 대신 converter_commentaries.converterCommentaries() 사용하면 됨.
     */
    // var commentariesUpdaterNotExistsLineup = schedule.scheduleJob('30 */1 * * *', function() {
    //     //converter_commentaries.converterCommentariesNotExistsLineup();
    // });


    /**
     * 라인업에는 있지만 players 테이블에 없는 선수
     * 코드 : CP
     */
    var playerUpdaterNotExistsLineup = schedule.scheduleJob('40 */4 * * *', function() {
        converter_players.convertPlayersLineupNotExists();
    });

    /**
     * 라인업에는 있지만 teams 테이블에 없는 팀 ( 당분간 중지 )
     * 코드 : CT
     */
    var teamUpdaterNotExistsLineup = schedule.scheduleJob('55 */6 * * *', function() {
        //converter_teams.convertTeamsNotExists();
    });

    /*----------------------------------------------------------------------*/

    // 일간 베스트 글 집계
    var bestContentsOfDay = schedule.scheduleJob('55 59 */1 * * *', function() {
        //collect_best_contents.collectBestContents('D');
    });

    // 주간 베스트 글 집계
    var bestContentsOfMonth = schedule.scheduleJob('55 59 23 * * */7', function() {
        //collect_best_contents.collectBestContents('W');
    });

    // 월간 베스트 글 집계
    var bestContentsOfMonth = schedule.scheduleJob('55 59 23 */30 * *', function() {
        //collect_best_contents.collectBestContents('M');
    });

    /*----------------------------------------------------------------------*/
    

    var futureMatchUpdater = schedule.scheduleJob('30 */1 * * *', function(){

        //matchupdate()
    }); 

    var endMatchUpdater = schedule.scheduleJob('5 */4 * * *', function(){
    
    });

    /*----------------------------------------------------------------------*/

    var playerUpdater = schedule.scheduleJob('30 */1 * * *', function(){
        // nuntingBotExcute()
    });

    var teamUpdater = schedule.scheduleJob('30 */1 * * *', function(){
         //nuntingBotExcute()
    });    
}


main()

