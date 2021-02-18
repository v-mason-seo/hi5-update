const db = require('./db');
const request = require('request');
const async = require('async');

const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');

/*-------------------------------------------------------------*/

exports.converterCommentaries = () => {

    console.log('[converterCommentaries]');

    // var now = new Date();

    // var log = {};
    // log.code = 'CC';
    // log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    // log.errcode = "history";
    // log.msg = "[시작] converterCommentaries 시작합니다.";

    var tasks = [
        function(callback00) {
            db.insertLog(log, function(err, result) {
                if ( err ) {
                    return callback00(err);
                }

                return callback00(null);
            })
        },
        function(callback01) {

            let query = 
            `
            select a.match_id
                ,  a.match_fa_id
                ,  a.match_date
                ,  a.localteam_id
                ,  ht.team_id home_team_id
                ,  a.visitorteam_id
                ,  at.team_id away_team_id
            from   matches a
                   left join teams ht on a.localteam_id = ht.team_fa_id
                   left join teams at on a.visitorteam_id = at.team_fa_id
                ,  competitions c
            where  a.status not in ('Postp.', 'Pen.', 'AET', 'Aban.')
            and    a.match_date between date_sub(now(), interval 2 day) and date_add(now(), interval 12 hour )
            and    a.comp_fa_id = c.comp_fa_id
            and    ifnull(c.is_active,0) = 1
            and    a.timer is null
            and    not exists ( select 'x'
                                from  lineup x
                                where a.localteam_id = x.team_fa_id
                                and   a.match_fa_id = x.match_fa_id )
            order by a.match_date desc                      
            `

            db.excuteSql(query, null, function (err, result){
                
                if (err) {
                    return callback01(err, null);
                } else {
                    return callback01(null, result);
                }
            });
        },
        function(matchList, callback02) {

            loopCommentaries(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterCommentaries] error : ' + err);
            log.code = 'CC';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] converterCommentaries 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
            //return 0;
        } else {
            console.log('[converterCommentaries] complete');
            log.code = 'CC';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] converterCommentaries 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
        }
    })
};

/**
 * 끝난 매치 코멘터리
 */
exports.converterEndMatchCommentaries = async function() {

    //------------------------------------------------------
    // Step1. 곧 다가올 매치 리스트를 쿼리한다.
    //------------------------------------------------------
    var matchList = await asyncGetFinishMatchs();

    for ( i = 0; i < matchList.length; i++) {
        
        var match = matchList[i];

        //------------------------------------------------------
        // Step2. Football API로 코멘터리를 요청한다.
        //------------------------------------------------------
        var matchData = await asyncRequestCommentaries(match.match_fa_id);
        
        if ( matchData == null ) {
            console.log("converterEndMatchCommentaries result : data null")
            continue;
        } else if ( matchData == 'E' ) {
            console.log("converterEndMatchCommentaries result : error")
            continue;
        } else if ( matchData == 'P' ) {
            console.log("converterEndMatchCommentaries result : pass")
            continue;
        } else if ( matchData == 'L') {
            // 요청제한
            console.log("-----converterEndMatchCommentaries 요청 제한으로 종료-----")
            return;
        }

        //------------------------------------------------------
        // Step3. 
        //------------------------------------------------------
        let commentaries = JSON.parse(matchData);
        let match_fa_id = match.match_fa_id;

        //let lineupList = commentaries.lineup;
        //let candidateList = commentaries.subs;
        //let substitutionList = commentaries.substitutions;
        //let match_id = match.match_id;

        if ( match_fa_id != 2323198) {
            // 4-1. 라인업 삭제
            await asyncDeleteLineup(match_fa_id);

            // 4-2. 라인업 입력
            await asyncInsertLineup(commentaries, match);
        }

        //4-3. 
        await asyncUpdateLineupSubstitutions(commentaries, match);

        //4-4.
        await asyncUpdateMatchStatFunc(commentaries);

        //4-5.
        await asyncUpdateLineupPlayerStat(commentaries, match);

        //4-6.
        await asyncUpdateLineupMatchId(match_fa_id);

        //4-8.
        await asyncUpdateLineupTeamId(match_fa_id);

        // players 마스터 테이블에 있는 정보로 lineup.player_id 정보 업데이트
        await asyncUpdateLineupPlayerId(match_fa_id);

        //5. 없는것들은 FA에 데이터 요청함.
        var emptyLineup = await asyncGetNotExistsPlayers(match.match_fa_id);

        for ( j = 0; j < emptyLineup.length; j++) {

            var playerData = await asyncRequestPlayer(emptyLineup[j].player_fa_id);

            if ( playerData == 'L' ) {
                // 리밋이 걸리면 무조건 종료
                console.log("-----converterEndMatchCommentaries 선수 데이터 요청 제한으로 종료-----")
                return;
            }

            if ( playerData == null || playerData == 'E' || playerData == 'P' ) {
                
                // 선수정보가 없으면 라인업 정보로 선수정보를 입력한다.
                var simplePlayer = [];
                simplePlayer.player_fa_id = emptyLineup[j].player_fa_id;
                simplePlayer.player_name = emptyLineup[j].player_name;
                simplePlayer.team_fa_id = emptyLineup[j].team_fa_id;
                simplePlayer.team_name = emptyLineup[j].team_name;
                simplePlayer.team_national_fa_id = emptyLineup[j].team_national_fa_id;
                simplePlayer.team_national_name = emptyLineup[j].team_national_name;

                await asyncInsertSimplePlayer(simplePlayer);
            } else {
                await asyncInsertPlayer(playerData);
            }
        }

        //4-7.
        await asyncUpdateLineupPlayerId(match_fa_id);

        console.log("index : " + i + " --- 끝");
    }

    console.log(" --- 끝 --- ");
}

async function asyncGetFinishMatchs() {
    
    let query = 
    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from   matches a
           left join teams ht on a.localteam_id = ht.team_fa_id
           left join teams at on a.visitorteam_id = at.team_fa_id
        ,  competitions c
    where  c.comp_fa_id = a.comp_fa_id
    and    ifnull(c.is_active, 0) = 1
	 and    now() between date_add(a.match_date, interval 1 hour) and date_add(a.match_date, interval 4 hour) 
     and    status = 'FT'
     #----------------------------
     #and    a.match_id != 252067
     #----------------------------
    ;      
    `
    return new Promise((resolve, reject) => {
        db.excuteSql(query, null, function (err, result){
        
            if (err) {
                console.log("asyncGetPlayersToDate 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });    
}


/**
 * 곧 다가오는 매치 
 *  - 경기시작시간 1시간 전부터는 15분 단위로 업데이트 한다.
 *  - 여기서는 선수정보가 없으면 FA 에 요청하고 인서트 한다.
 *  - FA 데이터 없으면 선수 아이디, 이름, 팀 정보만 입력한다.
 */
exports.converterUpCommingCommentaries = async function() {

    //------------------------------------------------------
    // Step1. 곧 다가올 매치 리스트를 쿼리한다.
    //------------------------------------------------------
    var matchList = await asyncGetUpcomingMatchs();

    for ( i = 0; i < matchList.length; i++) {
        
        var match = matchList[i];

        //------------------------------------------------------
        // Step2. Football API로 코멘터리를 요청한다.
        //------------------------------------------------------
        var matchData = await asyncRequestCommentaries(match.match_fa_id);
        
        if ( matchData == null ) {
            console.log("converterUpCommingCommentaries result : data null")
            continue;
        } else if ( matchData == 'E' ) {
            console.log("converterUpCommingCommentaries result : error")
            continue;
        } else if ( matchData == 'P' ) {
            console.log("converterUpCommingCommentaries result : pass")
            continue;
        } else if ( matchData == 'L') {
            // 요청제한
            console.log("-----converterUpCommingCommentaries 요청 제한으로 종료-----")
            return;
        }

        //------------------------------------------------------
        // Step3. 
        //------------------------------------------------------
        let commentaries = JSON.parse(matchData);
        let match_fa_id = match.match_fa_id;

        //let lineupList = commentaries.lineup;
        //let candidateList = commentaries.subs;
        //let substitutionList = commentaries.substitutions;
        //let match_id = match.match_id;

        // 4-1. 라인업 삭제
        await asyncDeleteLineup(match_fa_id);

        // 4-2. 라인업 입력
        await asyncInsertLineup(commentaries, match);

        //4-3. 
        await asyncUpdateLineupSubstitutions(commentaries, match);

        //4-4.
        await asyncUpdateMatchStatFunc(commentaries);

        //4-5.
        await asyncUpdateLineupPlayerStat(commentaries, match);

        //4-6.
        await asyncUpdateLineupMatchId(match_fa_id);

        //4-8.
        await asyncUpdateLineupTeamId(match_fa_id);

        // players 마스터 테이블에 있는 정보로 lineup.player_id 정보 업데이트
        await asyncUpdateLineupPlayerId(match_fa_id);

        //5. 없는것들은 FA에 데이터 요청함.
        var emptyLineup = await asyncGetNotExistsPlayers(match.match_fa_id);

        for ( j = 0; j < emptyLineup.length; j++) {

            var playerData = await asyncRequestPlayer(emptyLineup[j].player_fa_id);

            if ( playerData == 'L' ) {
                // 리밋이 걸리면 무조건 종료
                console.log("-----converterUpCommingCommentaries 선수 데이터 요청 제한으로 종료-----")
                return;
            }

            if ( playerData == null || playerData == 'E' || playerData == 'P' ) {
                
                // 선수정보가 없으면 라인업 정보로 선수정보를 입력한다.
                var simplePlayer = [];
                simplePlayer.player_fa_id = emptyLineup[j].player_fa_id;
                simplePlayer.player_name = emptyLineup[j].player_name;
                simplePlayer.team_fa_id = emptyLineup[j].team_fa_id;
                simplePlayer.team_name = emptyLineup[j].team_name;
                simplePlayer.team_national_fa_id = emptyLineup[j].team_national_fa_id;
                simplePlayer.team_national_name = emptyLineup[j].team_national_name;

                await asyncInsertSimplePlayer(simplePlayer);
            } else {
                await asyncInsertPlayer(playerData);
            }
        }

        //4-7.
        await asyncUpdateLineupPlayerId(match_fa_id);

        console.log("index : " + i + " --- 끝");
    }

    console.log(" --- 끝 --- ");
}

/**
 * Football API 에 선수 데이터를 요청한다.
 * 
 * @param {Football API 선수아이디} player_fa_id 
 */
let asyncRequestPlayer = (player_fa_id) => {

    return new Promise((resolve, reject) => {

        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : 'http://api.football-api.com/2.0/player/' + player_fa_id,
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                reject('E'); // error
            } else if ( !error && response.statusCode != 200 ) {

                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestPlayer " + player_fa_id + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestPlayer " + player_fa_id + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }
                
            } else {
                console.log("asyncRequestPlayer " + player_fa_id + " 선수 요청 완료")
                resolve(body);
            }
        })
    });
}

function asyncInsertPlayer(player_data) {

    return new Promise((resolve, reject) => {

        // if ( player_data == 'next' ) {
        //     reject('next')
        // }

        let player = JSON.parse(player_data);
        let playerParamter = []

        let birthdate;
        try {
            birthdate = stringToDate(player.birthdate,"dd/MM/yyyy","/").toISOString().slice(0, 19).replace('T', ' ')
            if ( birthdate.length > 10) {
                birthdate = birthdate.substring(0, 10);
            }
        } catch(err) {
            birthdate = undefined;
        }

        let common_name;
        if ( player.common_name) {
            common_name = player.common_name.replace("'", "\\'")
        } else {
            common_name = undefined;
        }

        playerParamter.push([player.id,
                            player.name.replace("'", "\\'"),
                            common_name,
                            player.firstname != null && player.firstname.replace("'", "\\'"),
                            player.lastname != null && player.lastname.replace("'", "\\'").substring(0, 30),
                            player.team != null && player.team.replace("\n", "").trim() ? player.team.replace("\n", "").trim() : undefined,
                            player.teamid != null && player.teamid.replace("\n", "").trim() ? player.teamid.replace("\n", "").trim() : undefined,
                            player.nationality,
                            birthdate ? birthdate : undefined,
                            player.age ? player.age : undefined,
                            player.birthcountry,
                            player.birthplace,
                            player.position,
                            (player.height != null && player.height.replace("\n", "").trim() ) ? player.height.replace("\n", "").trim() : undefined,
                            (player.weight != null && player.weight.replace("\n", "").trim() ) ? player.weight.replace("\n", "").trim() : undefined,
                            player.player_statistics ? JSON.stringify(player.player_statistics) : undefined,
                            0,            //retry_count
                            0   // is_user_add
                        ]);

        let query =
        `
        INSERT INTO players 
        (   player_fa_id,
            player_name,
            player_common_name,
            player_firstname,
            player_lastname,
            team,
            teamid,
            nationality,
            birthdate,
            age,
            birthcountry,
            birthplace,
            position,
            height,
            weight,
            player_statistics,
            retry_count,
            is_user_add) 
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
            player_name        = VALUES(player_name),
            player_common_name = VALUES(player_common_name),
            player_firstname   = VALUES(player_firstname),
            player_lastname    = VALUES(player_lastname),
            team               = VALUES(team),
            teamid             = VALUES(teamid),
            nationality        = VALUES(nationality),
            birthdate          = VALUES(birthdate),
            birthcountry       = VALUES(birthcountry),
            birthplace         = VALUES(birthplace),
            height             = VALUES(height),
            weight             = VALUES(weight),
            age                = VALUES(age),
            position           = VALUES(position),
            player_statistics  = VALUES(player_statistics),
            updated            = now(),
            retry_count        = 0,
            is_user_add        = VALUES(is_user_add)
            ;
        `

        db.excuteSql(query, [playerParamter], (err, result)=>{
            if (err) {
                // console.log(' - [insertPlayer] error : ' + err);

                // var log = {};
                // log.code = 'CP';
                // log.subcode = player.id;
                // log.errcode = err.errno;
                // log.msg = err.message;
                // db.insertLog(log, function(err, result){
                //     callback(null, null);
                // })
                return reject(err)
                
            } else {
                //callback(null, result);
                console.log("asyncInsertPlayer " + player.id + " 데이터 입력 완료")
                return resolve(result)
            }
        })

    });
}

async function asyncGetNotExistsPlayers(match_fa_id) {
    
    let query = 
    `
    select a.player_fa_id
        ,  a.player_name
        ,  case when t.is_national != 1 then t.team_fa_id else null end team_fa_id
        ,  case when t.is_national != 1 then t.team_name  else null end team_name
        ,  case when t.is_national  = 1 then t.team_fa_id else null end team_national_fa_id
        ,  case when t.is_national  = 1 then t.team_name  else null end team_national_name
    from   lineup a
           left join teams t on a.team_id = t.team_id
    where  a.match_fa_id = ?
    and    not exists ( select 'x' from 
                        players p 
                        where   a.player_fa_id = p.player_fa_id 
                        and     ifnull(p.is_user_add, 0) < 1)
    `

    return new Promise((resolve, reject) => {
        db.excuteSql(query, [match_fa_id], function (err, result){
        
            if (err) {
                console.log("asyncGetPlayersToDate 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });    
}

async function asyncInsertSimplePlayer(player) {

    return new Promise((resolve, reject) => {

        let playerParamter = []

        playerParamter.push([player.player_fa_id,
                            player.player_name ? player.player_name.replace("'", "\\'") : undefined,
                            player.player_name ? player.player_name.replace("'", "\\'") : undefined,
                            player.team_name ? player.team_name.replace("'", "\\'") : undefined,
                            player.team_fa_id,
                            player.team_national_fa_id,
                            player.team_national_name,
                            1
                        ]);

        let query =
        `
        INSERT INTO players 
        (   player_fa_id,
            player_common_name,
            player_name,
            team,
            teamid,
            national_teamid,
            nationality,
            is_user_add
        ) 
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
            player_common_name = VALUES(player_common_name),
            player_name = VALUES(player_name),
            team = VALUES(team),
            teamid = VALUES(teamid),
            national_teamid       = VALUES(national_teamid),
            nationality       = VALUES(nationality),
            updated           = now(),
            retry_count       = retry_count + 1
            ;
        `

        db.excuteSql(query, [playerParamter], (err, result)=>{
            if (err) {
                reject(err)
            } else {
                //callback(null, result);
                console.log("asyncInsertSimplePlayer " + player.player_name + " 데이터 입력 완료")
                resolve(result)
            }
        })
    });
};

/**
 * 곧 다가올 매치 리스트 가져오기
 */
let asyncGetUpcomingMatchs = ()=>{
    
    let query = 
    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from   matches a
           left join teams ht on a.localteam_id = ht.team_fa_id
           left join teams at on a.visitorteam_id = at.team_fa_id
        ,  competitions c
    where  a.status not in ('Postp.', 'Pen.', 'AET', 'Aban.')
    and    now() between date_sub(a.match_date, interval 1 hour) and a.match_date
    and    a.comp_fa_id = c.comp_fa_id
    and    ifnull(c.is_active,0) = 1
    and    a.timer is null
    and    not exists ( select 'x'
                        from  lineup x
                        where a.match_fa_id = x.match_fa_id )
    order by a.match_date desc      
    `

    // let query = 
    // `
    // select a.match_id
    //     ,  a.match_fa_id
    //     ,  a.match_date
    //     ,  a.localteam_id
    //     ,  ht.team_id home_team_id
    //     ,  a.visitorteam_id
    //     ,  at.team_id away_team_id
    // from   matches a
    //        left join teams ht on a.localteam_id   = ht.team_fa_id
    //        left join teams at on a.visitorteam_id = at.team_fa_id
    //     ,  competitions c           
    // where  a.comp_fa_id = c.comp_fa_id
    // and    ifnull(c.is_active, 0) = 1
    // and    c.comp_fa_id = 1056
    // #and    a.match_fa_id = 2323198
    // `

    return new Promise((resolve, reject) => {
        db.excuteSql(query, null, function (err, result){
        
            if (err) {
                console.log("asyncGetPlayersToDate 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });    
}





/**
 * 곧 시작할 경기
 */
exports.converterSoonCommentaries_old = () => {

    console.log('[converterCommentaries]');

    var now = new Date();

    var log = {};
    log.code = 'CC';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] converterCommentaries 시작합니다.";

    var tasks = [
        function(callback00) {
            db.insertLog(log, function(err, result) {
                if ( err ) {
                    return callback00(err);
                }

                return callback00(null);
            })
        },
        function(callback01) {

            let query = 
            `
            select a.match_id
                ,  a.match_fa_id
                ,  a.match_date
                ,  a.localteam_id
                ,  ht.team_id home_team_id
                ,  a.visitorteam_id
                ,  at.team_id away_team_id
            from   matches a
                   left join teams ht on a.localteam_id = ht.team_fa_id
                   left join teams at on a.visitorteam_id = at.team_fa_id
                ,  competitions c
            where  a.status not in ('Postp.', 'Pen.', 'AET', 'Aban.')
            and    now() between date_sub(a.match_date, interval 1 hour) and a.match_date
            and    a.comp_fa_id = c.comp_fa_id
            and    ifnull(c.is_active,0) = 1
            and    a.timer is null
            and    not exists ( select 'x'
                                from  lineup x
                                where a.localteam_id = x.team_fa_id
                                and   a.match_fa_id = x.match_fa_id )
            order by a.match_date desc                      
            `

            db.excuteSql(query, null, function (err, result){
                
                if (err) {
                    return callback01(err, null);
                } else {
                    return callback01(null, result);
                }
            });
        },
        function(matchList, callback02) {

            loopCommentaries(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterCommentaries] error : ' + err);
            log.code = 'CC';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] converterCommentaries 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
            //return 0;
        } else {
            console.log('[converterCommentaries] complete');
            log.code = 'CC';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] converterCommentaries 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
        }
    })
};


exports.converterLiveCommentaries = function() {

    console.log('[converterLiveCommentaries]');

    var now = new Date();

    var log = {};
    log.code = 'CC';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] converterLiveCommentaries 시작합니다.";

    var tasks = [
        // function(callback00) {
        //     db.insertLog(log, function(err, result) {
        //         if ( err ) {
        //             return callback00(err);
        //         }

        //         return callback00(null);
        //     })
        // },
        function(callback01) {
            let query = 
            `
            select a.match_id
                ,  a.match_fa_id
                ,  a.match_date
                ,  a.localteam_id
                ,  ht.team_id home_team_id
                ,  a.visitorteam_id
                ,  at.team_id away_team_id
            from   matches a
                   left join teams ht on a.localteam_id = ht.team_fa_id
                   left join teams at on a.visitorteam_id = at.team_fa_id
                ,  competitions c
            where  a.status not in ('FT', 'Postp.')
            and    a.comp_fa_id = c.comp_fa_id
            and    ifnull(c.is_active,0) = 1
            and    a.timer is not null
            and    now() between date_sub(a.match_date, interval 1 minute) 
                             and date_add(a.match_date, interval 160 minute)
            order by a.match_date desc  
            `

    let parameter = [];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback01(err, null);
        } else {
            return callback01(null, result);
        }
    });
        },
        function(matchList, callback02) {

            loopCommentaries(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterLiveCommentaries] error : ' + err);
            log.code = 'CC';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] converterLiveCommentaries 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
            //return 0;
        } else {
            // console.log('[converterLiveCommentaries] complete');
            // log.code = 'CC';
            // log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            // log.errcode = "history";
            // log.msg = "[종료] converterLiveCommentaries 종료합니다.";

            // db.insertLog(log, function(err, result) {
            //     return 1;
            // })

            return 1;
        }
    })
};


exports.converterCommentariesToId = (match_fa_id) => {

    console.log('[converterCommentariesToId]');

    var tasks = [
        function(callback01) {
            getMatchInfo(match_fa_id, function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }

                return callback01(null, result);
            })
        },
        function(matchList, callback02) {

            loopCommentaries(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterCommentaries] error : ' + err);
            return 0;
        } else {
            console.log('[converterCommentaries] complete');
            return 1;
        }
    })
};

exports.converterCommentariesNotExistsLineup = () => {

    console.log('[converterCommentaries]');

    var tasks = [
        function(callback01) {
            getCommentariesTagetNotExistsLineup(function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }

                return callback01(null, result);
            })
        },
        function(matchList, callback02) {

            loopCommentaries(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterCommentaries] error : ' + err);
            return 0;
        } else {
            console.log('[converterCommentaries] complete');
            return 1;
        }
    })
};


/*-------------------------------------------------------------*/

function loopCommentaries(commentaries_list, callback) {

    let index = 0;

    async.whilst (
        function(callback01) {
            return index < commentaries_list.length;
        },
        function(callback02) {

            //let match_id = commentaries_list[index].match_fa_id;
            //let local_id = commentaries_list[index].localteam_id;
            //let visitor_id = commentaries_list[index].visitorteam_id;
            console.log(' - [loopCommentaries] ' + (index + 1) + ' / ' + commentaries_list.length 
                + ', date : ' + commentaries_list[index].match_date
                + ', match fa id : ' + commentaries_list[index].match_fa_id 
                + ', match id : ' + commentaries_list[index].match_id);
            
            let match_data = commentaries_list[index];    
            index++;

            request_insert_commentaries(match_data/*match_id, local_id, visitor_id*/, function(err, result) {
                if (err) {
                    return callback02(err, null);
                } else {
                    return callback02(null, result);
                } 
            })
        },
        function(err, result) {
            if (err) {
                return callback(err, null);
            } else {
                return callback(null, result);
            } 
        }
    )
}

function request_insert_commentaries(match_data/*match_id, localteam_id, visitorteam_id*/, callback) {

    var tasks = [
        function(callback01) {
            requestCommentaries(match_data.match_fa_id, function(err, result) { 
                if ( err ) {
                    return callback01(err, null);
                }

                return callback01(null, result);
            } )
        },
        function(commentaries_data, callback02) {

            if ( commentaries_data == 'next' ) {
                console.log(' - requst commentaries data empty!');
                return callback02(null, commentaries_data);
            }

            CommentariesFunc(commentaries_data, match_data/*match_id, localteam_id, visitorteam_id*/, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            return callback(err, null);    
        }

        return callback(null, result);;
        
    })
}

function getCommentariesTaget(callback) {

    console.log(' - [getStandingTaget]');

    let query = 
    // `
    // select a.match_id
    //     ,  a.match_fa_id
    //     ,  a.match_date
    //     ,  a.localteam_id
    //     ,  ht.team_id home_team_id
    //     ,  a.visitorteam_id
    //     ,  at.team_id away_team_id
    // from   matches a
    //        left join teams ht on a.localteam_id = ht.team_fa_id
    //        left join teams at on a.visitorteam_id = at.team_fa_id
    // where  a.status not in ('Postp.', 'Pen.', 'AET', 'TBA', 'Aban.')
    // and    a.match_date < date_add(now(), interval 2 hour )
    // and    not exists ( select 'x'
    //                     from  lineup x
    //                     where a.localteam_id = x.team_fa_id
    //                     and   a.match_fa_id = x.match_fa_id )
    // and    not exists ( select 'x'
    //                     from convert_log x
    //                     where x.code = 'CCMD'
    //                     and   x.subcode = a.match_fa_id
    //                     and   x.errcode = 404
    //                     and   a.comp_fa_id not in ('1229', '1221', '1399', '1269', '1204'))                    
    // order by a.match_date desc                      
    // `

    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from   matches a
           left join teams ht on a.localteam_id = ht.team_fa_id
           left join teams at on a.visitorteam_id = at.team_fa_id
        ,  competitions c
    where  a.status not in ('Postp.', 'Pen.', 'AET', 'Aban.')
    and    a.match_date between date_sub(now(), interval 2 day) and date_add(now(), interval 24 hour )
    and    a.comp_fa_id = c.comp_fa_id
    and    ifnull(c.is_active,0) = 1
    and    not exists ( select 'x'
                        from  lineup x
                        where a.localteam_id = x.team_fa_id
                        and   a.match_fa_id = x.match_fa_id )
    order by a.match_date desc                      
    `

    db.excuteSql(query, null, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}


function getMatchInfo(mfaid, callback) {

    console.log(' - [getMatchInfo]');

    let query = 
    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from   matches a
           left join teams ht on a.localteam_id = ht.team_fa_id
           left join teams at on a.visitorteam_id = at.team_fa_id
    where  a.match_fa_id = ?
    `

    let parameter = [mfaid];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}

function getLiveMatch(callback) {

    console.log(' - [getLiveMatch]');

    let query = 
    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from   matches a
           left join teams ht on a.localteam_id = ht.team_fa_id
           left join teams at on a.visitorteam_id = at.team_fa_id
        ,  competitions c
    where  a.status not in ('FT', 'Postp.')
    and    a.comp_fa_id = c.comp_fa_id
    and    ifnull(c.is_active,0) = 1
    and    now() between date_sub(a.match_date, interval 5 minute) 
                     and date_add(a.match_date, interval 120 minute)
    order by a.match_date desc  
    `

    let parameter = [];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}

function getCommentariesTagetNotExistsLineup(callback) {

    console.log(' - [getCommentariesTagetNotExistsLineup]');

    let query = 
    `
    select a.match_id
        ,  a.match_fa_id
        ,  a.match_date
        ,  a.localteam_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from matches a
         left join teams ht on a.localteam_id = ht.team_fa_id
         left join teams at on a.visitorteam_id = at.team_fa_id
      ,  competitions c
    where match_date < now()
    and   a.status not in ('Postp')
    and   not exists ( select 'x' from lineup b where a.match_fa_id = b.match_fa_id)
    and   not exists ( select 'x' from convert_log b where b.errcode = '404' and b.code = 'CCMD' and b.subcode = a.match_fa_id)
    and   a.comp_fa_id = c.comp_fa_id
    and   ifnull(c.is_active,0) = 1
    #and a.comp_fa_id not in (1093, 1425, 1428, 1232, 1457)
    order by a.match_date desc
    ;
    `

    db.excuteSql(query, null, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}


/**
 * Football API 에 코멘터리(라인업, 매치 이벤트 등 ) 데이터를 요청한다.
 * @param {*} match_fa_id 매치 아이디
 */
async function asyncRequestCommentaries(match_fa_id) {
    
    return new Promise((resolve, reject) => {

        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : 'http://api.football-api.com/2.0/commentaries/' + match_fa_id,
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                reject('E'); // error
            } else if ( !error && response.statusCode != 200 ) {

                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestCommentaries " + match_fa_id + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestCommentaries " + match_fa_id + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }
                
            } else {
                console.log("asyncRequestCommentaries " + match_fa_id + " 코멘터리 요청 완료")
                resolve(body);
            }
        })
    });
}


function requestCommentaries(match_id/*, localteam_id, visitorteam_id*/, callback) {
    
    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : 'http://api.football-api.com/2.0/commentaries/' + match_id,
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {
        // if (!error && response.statusCode == 200) {
        //     return callback(null, body);
        // } else if (!error && response.statusCode == 404) {
        //     return callback(null, 'empty');
        // } else {
        //     return callback(error, null);
        // }

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            var log = {};
            log.code = 'CCMD';
            log.subcode = match_id;
            log.errcode = response.statusCode;
            log.msg = JSON.parse(body).err ? JSON.parse(body).err : JSON.parse(body).message;

            db.insertLog(log, function(err, result) {
                
                return callback(null, 'next');
            })
        } else {
            return callback(null, body);
        }
    })
}


/**
 * convertCommentaries - 1
 * @param {*} body 
 * @param {*} match_id 
 * @param {*} localteam_id 
 * @param {*} visitorteam_id 
 * @param {*} mainCallback 
 */
function CommentariesFunc(body, match_data/*match_id, localteam_id, visitorteam_id*/, mainCallback) {
    
    let commentaries = JSON.parse(body);
    
    let lineupList = commentaries.lineup;
    let candidateList = commentaries.subs;
    let substitutionList = commentaries.substitutions;

    async.series([
        // 1. Delete
        function (callback) {
            DeleteLineupFunc(match_data.match_fa_id/*commentaries.match_id*/, function(err, result) {
                if ( err ) {
                    return callback(err, null);
                }
                return callback(null, result);
            })
        },
        // 2. Insert
        function (callback) {
            InsertLineupFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, function(err, result) {
                if ( err ) {
                    return callback(err, null);
                }
                return callback(null, result);
            })
        },
        // 3. Update
        function (callback) {
            UpdateLineupSubstitutionsFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        },
        // 4. Update match_stats UpdateMatchStatFunc
        function (callback) {
            UpdateMatchStatFunc(commentaries, function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        },
        // 5. Update player stats
        function (callback) {
            UpdateLineupPlayerStatFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        },
        // 6. Update Match id
        function ( callback) {
            UpdateLineupMatchId(match_data.match_fa_id , function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        },
        // 7. Update Player Id
        function ( callback) {
            UpdateLineupPlayerId(match_data.match_fa_id , function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        },
        // 8. Update Team Id
        function ( callback) {
            UpdateLineupTeamId(match_data.match_fa_id , function(err, result) {
                // if ( err ) {
                //     return callback(err, null);
                // }
                return callback(null, result);
            })
        }                
    ], function (err, results) {
        console.log('Commentaries complete');
        if ( err ) return mainCallback(err, null);

        return mainCallback(null, results);
    });
}


/**
 * 1. 라인업 데이터 삭제
 * @param {*} match_fa_id 
 */
async function asyncDeleteLineup(match_fa_id) {
 
    let query =
    `
    DELETE FROM lineup
    where match_fa_id = ?
    ;
    `

    let parameter = [ match_fa_id ];

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log("asyncDeleteLineup 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

/**
 * 2.
 * @param {*} commentaries 
 * @param {*} match_data 
 */
async function asyncInsertLineup(commentaries, match_data) {
 
    let query =
    `
    replace INTO lineup ( match_fa_id, team_fa_id, player_fa_id, match_id, team_id, number, player_name, pos, startingyn )
    VALUES ?
    ;
    `

    let lineupParameter = [];

    if (commentaries.lineup && commentaries.lineup.localteam) {
        let lineupList = commentaries.lineup;
        lineupList.localteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "Y";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.localteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.home_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    
    if (commentaries.lineup && commentaries.lineup.visitorteam) {
        let lineupList = commentaries.lineup;
        lineupList.visitorteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "Y";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.visitorteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.away_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    
    if (commentaries.subs && commentaries.subs.localteam) {
        let candidateList = commentaries.subs;
        candidateList.localteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "N";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.localteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.home_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    

    if (commentaries.subs && commentaries.subs.visitorteam) {
        let candidateList = commentaries.subs;
        candidateList.visitorteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "N";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.visitorteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.away_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }

    return new Promise((resolve, reject) => {

        // 데이터가 없으면 함수를 빠져나간다.
        if ( lineupParameter.length == 0 ) {
            console.log("match_id" + match_data.match_id + " asyncInsertLineup 데이터 없음")
            return resolve("P");
            
        }

        // 데이터 입력
        db.excuteSql(query, [lineupParameter], function (err, result){
        
            if (err) {
                console.log("asyncInsertLineup 실패 - " + err)
                return reject(err);
            } else {
                return resolve(result);
            }
        });
    });
}

/**
 * 3.
 * @param {*} commentaries 
 * @param {*} match_data 
 */
async function asyncUpdateLineupSubstitutions(commentaries, match_data) {
    let update_query = 
    `
    update lineup
    set substitutions = {0}, subs_minute = {1}
    where match_fa_id = {2} and team_fa_id = {3} and player_fa_id = {4}
    ;
    `

    let composite_update_query = '';

    let substitutionList = commentaries.substitutions;

    if (commentaries.substitutions && commentaries.substitutions.localteam ) {
        substitutionList.localteam.forEach(function(element) {
            
            if ( element.on_id != undefined && element.on_id != "" ) {
                composite_update_query 
                    += update_query.format("'on'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.localteam_id, element.on_id);
            }
            
            if ( element.off_id != undefined && element.off_id != "" ) {
                composite_update_query 
                    += update_query.format("'off'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.localteam_id, element.off_id);
            }
            
        }, this);
    }

    if (commentaries.substitutions && commentaries.substitutions.visitorteam ) {
        substitutionList.visitorteam.forEach(function(element) {
            if ( element.on_id != undefined && element.on_id != "" ) {
                composite_update_query 
                    += update_query.format("'on'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.visitorteam_id, element.on_id);
            }
            
            if ( element.off_id != undefined && element.off_id != "" ) {
                composite_update_query 
                    += update_query.format("'off'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.visitorteam_id, element.off_id);
            }
        }, this);
    }

    return new Promise((resolve, reject) => {
        
        // // 데이터가 없으면 함수를 빠져나간다.
        if ( !composite_update_query) {
            console.log("match_id" + match_data.match_id + " asyncUpdateLineupSubstitutions 데이터 없음")
            //console.log(' - (3) Update lineup query was empty (UpdateLineupSubstitutionsFunc())');
            return resolve("P");
        }

        db.excuteSql(composite_update_query, null, function (err, result){
        
            if (err) {
                console.log("asyncUpdateLineupSubstitutions 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

/*======================================================================*/

/**
 * CommentariesFunc - 4
 * @param {*} commentaries 
 * @param {*} callback 
 */
async function asyncUpdateMatchStatFunc(commentaries, callback) {
    
    let update_match_stat_query =
    `
    update matches
    set    localteam_stats = ?
        ,  visitorteam_stats = ?
        ,  api_comments = ?
    where  match_fa_id = ?
    ;
    `

    return new Promise((resolve, reject) => {

        if ( commentaries == null 
            || commentaries.match_stats == null 
            || commentaries.match_stats.localteam == null
            || commentaries.match_stats.visitorteam == null ) {
                
                console.log(' - (4) asyncUpdateMatchStatFunc commentaries.match_stats is empty!');
                return resolve("P");
        }

        var match_stats_parameter 
            = [ commentaries.match_stats.localteam ? JSON.stringify(commentaries.match_stats.localteam) : undefined,
                commentaries.match_stats.visitorteam ? JSON.stringify(commentaries.match_stats.visitorteam) : undefined,
                commentaries.comments ? JSON.stringify(commentaries.comments) : undefined,
                commentaries.match_id ]

        db.excuteSql(update_match_stat_query, match_stats_parameter, function (err, result){
        
            if (err) {
                console.log(' - (4) Update match_stats error : ' + err);
                reject(err);
            } else {
                console.log(' - (4) Update match_stats complete : ' + result.affectedRows);
                resolve(result);
            }
        });
    });   
}

/**
 * CommentariesFunc - 5
 * @param {*} commentaries 
 * @param {*} localteam_id 
 * @param {*} visitorteam_id 
 * @param {*} callback 
 */
async function asyncUpdateLineupPlayerStat(commentaries, match_data) {
    
    let composite_player_stats_update_query = ''
    let base_player_stats_update_query = 
    `
    update lineup
    set player_stats = '{0}'
    where match_fa_id = {1} and team_fa_id = {2} and player_fa_id = {3};
    `

    if (commentaries.player_stats
        && commentaries.player_stats.localteam
        && commentaries.player_stats.localteam.player ) {

            let localteam_stats = commentaries.player_stats.localteam.player
            localteam_stats.forEach(function(element) {
                
                        if ( element && element.id )  {
                            composite_player_stats_update_query 
                            += base_player_stats_update_query.format(JSON.stringify(element).replace("'", "\\'"), commentaries.match_id, match_data.localteam_id, element.id );
                        }
                    }, this);
        }

    if (commentaries.player_stats
        && commentaries.player_stats.visitorteam
        && commentaries.player_stats.visitorteam.player ) {

            let visitorteam_stats = commentaries.player_stats.visitorteam.player;
            visitorteam_stats.forEach(function(element) {
                
                if ( element && element.id )  {
                    composite_player_stats_update_query 
                    += base_player_stats_update_query.format(JSON.stringify(element).replace("'", "\\'"), commentaries.match_id, match_data.visitorteam_id, element.id );
                }
            }, this);

        }

    return new Promise((resolve, reject) => {

        // 에러처리 필요함.
        if ( !composite_player_stats_update_query) {
            console.log(' - (5) Update player stats query was empty');
            return resolve('P');
        }

        db.excuteSql(composite_player_stats_update_query, null, function (err, result){
        
            if (err) {
                console.log(' - (5) Update player stats error : ' + err);
                return reject(err);
            } else {
                console.log(' - (5) Update player stats complete : ' + result.length);
                return resolve(result);
            }
        });
    });
}


/**
 * 6.
 * @param {*} match_id //match_fa_id
  */
async function asyncUpdateLineupMatchId(match_id) {
    let query =
    `
    update lineup
    set match_id      = ( select match_id from matches where match_fa_id = ?)
    where match_fa_id = ?
     and match_id is null
    ;
    `

    var parameter = [ match_id, match_id]

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log(' - (6) Update UpdateLineupMatchId error : ' + err);
                reject(err);
            } else {
                console.log(' - (6) Update UpdateLineupMatchId complete : ' + result.affectedRows);
                resolve(result);
            }
        });
    });
}


/**
 * 
 * @param {*} match_id 
 */
async function asyncUpdateLineupPlayerId(match_id) {
    
    let query =
    `
    update lineup l
    set    player_id   = (select player_id from players where player_fa_id = l.player_fa_id)
    where  match_fa_id = ?
    and    player_id is null;   
    ;
    `

    var parameter = [ match_id]

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log(' - (7) Update UpdateLineupPlayerId error : ' + err);
                reject(err);
            } else {
                console.log(' - (7) Update UpdateLineupPlayerId complete : ' + result.affectedRows);
                resolve(result);
            }
        });
    });
}


/**
 * 8.
 * @param {*} match_id 
  */
async function asyncUpdateLineupTeamId(match_id) {
    
    let query =
    `
    update lineup l
    set    team_id     = (select team_id from teams where team_fa_id = l.team_fa_id)
    where  match_fa_id = ?
    and    team_id is null;
    ;
    `

    var parameter = [ match_id]

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log(' - (8) Update UpdateLineupTeamId error : ' + err);
                reject(err);
            } else {
                console.log(' - (8) Update UpdateLineupTeamId complete : ' + result.affectedRows);
                resolve(result);
            }
        });
    });
}


/*======================================================================*/



/**
 * CommentariesFunc - 1 
 * @param {*} match_id 
 * @param {*} callback 
 */
function DeleteLineupFunc(match_id, callback) {
    
    let delete_lineup_query =
    `
    DELETE FROM lineup
    where match_fa_id = ?
    ;
    `

    db.excuteSql(delete_lineup_query, [match_id], (err, result)=>{
        if (err) {
            console.log(' - (1) Delete matches error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (1) Delete matches complete : ' + result.info);
            return callback(null, result);
        }
    })
}

/**
 * CommentariesFunc - 2
 * @param {*} commentaries 
 * @param {*} localteam_id 
 * @param {*} visitorteam_id 
 * @param {*} callback 
 */
function InsertLineupFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, callback) {
  
    let query =
    `
    replace INTO lineup ( match_fa_id, team_fa_id, player_fa_id, match_id, team_id, number, player_name, pos, startingyn )
    VALUES ?
    ;
    `

    let lineupParameter = [];
    
    // if (commentaries.lineup && commentaries.lineup.localteam) {
    //     let lineupList = commentaries.lineup;
    //     lineupList.localteam.forEach(function(lineup) {
            
    //         if ( lineup.id ) {
    //             lineup.starting = "Y";
    //             lineupParameter.push(
    //                 [ commentaries.match_id, "" +
    //                     match_data.localteam_id, "" +
    //                     lineup.id, "" +
    //                     match_data.match_id, "" +
    //                     match_data.home_team_id, "" +
    //                     lineup.number, "" +
    //                     lineup.name, "" +
    //                     lineup.pos, "" + 
    //                     lineup.starting
    //             ]);
    //         }
    //     }, this);
    // }

    if (commentaries.lineup && commentaries.lineup.localteam) {
        let lineupList = commentaries.lineup;
        lineupList.localteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "Y";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.localteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.home_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    
    // if (commentaries.lineup && commentaries.lineup.visitorteam) {
    //     let lineupList = commentaries.lineup;
    //     lineupList.visitorteam.forEach(function(lineup) {
            
    //         if ( lineup.id ) {
    //             lineup.starting = "Y";
    //             lineupParameter.push(
    //                 [ commentaries.match_id, "" +
    //                     match_data.visitorteam_id, "" +
    //                     lineup.id, "" +
    //                     match_data.match_id, "" +
    //                     match_data.away_team_id, "" +
    //                     lineup.number, "" +
    //                     lineup.name, "" +
    //                     lineup.pos, "" + 
    //                     lineup.starting
    //             ]);
    //         }
    //     }, this);
    // }

    if (commentaries.lineup && commentaries.lineup.visitorteam) {
        let lineupList = commentaries.lineup;
        lineupList.visitorteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "Y";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.visitorteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.away_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    
    // if (commentaries.subs && commentaries.subs.localteam) {
    //     let candidateList = commentaries.subs;
    //     candidateList.localteam.forEach(function(lineup) {
            
    //         if ( lineup.id ) {
    //             lineup.starting = "N";
    //             lineupParameter.push(
    //                 [ commentaries.match_id, "" +
    //                     match_data.localteam_id, "" +
    //                     lineup.id, "" +
    //                     match_data.match_id, "" +
    //                     match_data.home_team_id, "" +
    //                     lineup.number, "" +
    //                     lineup.name, "" +
    //                     lineup.pos, "" + 
    //                     lineup.starting
    //             ]);
    //         }
    //     }, this);
    // }

    if (commentaries.subs && commentaries.subs.localteam) {
        let candidateList = commentaries.subs;
        candidateList.localteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "N";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.localteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.home_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }
    
    // if (commentaries.subs && commentaries.subs.visitorteam) {
    //     let candidateList = commentaries.subs;
    //     candidateList.visitorteam.forEach(function(lineup) {
            
    //         if ( lineup.id ) {
    //             lineup.starting = "N";
    //             lineupParameter.push(
    //                 [ commentaries.match_id, "" +
    //                     match_data.visitorteam_id, "" +
    //                     lineup.id, "" +
    //                     match_data.match_id, "" +
    //                     match_data.away_team_id, "" +
    //                     lineup.number, "" +
    //                     lineup.name, "" +
    //                     lineup.pos, "" + 
    //                     lineup.starting
    //             ]);
    //         }
    //     }, this);
    // }            

    if (commentaries.subs && commentaries.subs.visitorteam) {
        let candidateList = commentaries.subs;
        candidateList.visitorteam.forEach(function(lineup) {
            
            if ( lineup.id ) {
                lineup.starting = "N";
                lineupParameter.push(
                    [ commentaries.match_id,
                        match_data.visitorteam_id,
                        lineup.id,
                        match_data.match_id,
                        match_data.away_team_id,
                        lineup.number,
                        lineup.name,
                        lineup.pos,
                        lineup.starting
                ]);
            }
        }, this);
    }

    if ( lineupParameter.length == 0 ) {
        console.log(' - (2) Insert lineup parameter empty (InsertLineupFunc)');

        var log = {};
            log.code = 'CCMD';
            log.subcode = match_data.match_fa_id;
            log.errcode = 404;
            log.msg = 'lineup data empty, match_fa_id : ' + match_data.match_id;

            db.insertLog(log, function(err, result) {
                
                //return callback(null, 'next');
                return callback(null, ' - (2) Insert lineup parameter empty');
            })

    } else {
        db.excuteSql(query, [lineupParameter], (err, result)=>{
            if (err) {
                console.log(' - (2) Insert lineup error : ' + err);
                return callback(err, null);
            } else {
                console.log(' - (2) Insert lineup complete : ' + result.info);
                return callback(null, result);
            }
        })
    }
}


/**
 * CommentariesFunc - 3
 * @param {*} commentaries 
 * @param {*} localteam_id 
 * @param {*} visitorteam_id 
 * @param {*} callback 
 */
function UpdateLineupSubstitutionsFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, callback) {
    
    let update_query = 
    `
    update lineup
    set substitutions = {0}, subs_minute = {1}
    where match_fa_id = {2} and team_fa_id = {3} and player_fa_id = {4}
    ;
    `

    let composite_update_query = '';

    let substitutionList = commentaries.substitutions;

    if (commentaries.substitutions && commentaries.substitutions.localteam ) {
        substitutionList.localteam.forEach(function(element) {
            
            if ( element.on_id != undefined && element.on_id != "" ) {
                composite_update_query 
                    += update_query.format("'on'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.localteam_id, element.on_id);
            }
            
            if ( element.off_id != undefined && element.off_id != "" ) {
                composite_update_query 
                    += update_query.format("'off'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.localteam_id, element.off_id);
            }
            
        }, this);
    }

    if (commentaries.substitutions && commentaries.substitutions.visitorteam ) {
        substitutionList.visitorteam.forEach(function(element) {
            if ( element.on_id != undefined && element.on_id != "" ) {
                composite_update_query 
                    += update_query.format("'on'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.visitorteam_id, element.on_id);
            }
            
            if ( element.off_id != undefined && element.off_id != "" ) {
                composite_update_query 
                    += update_query.format("'off'", element.minute ? "'" + element.minute + "'" : "\'\'", commentaries.match_id, match_data.visitorteam_id, element.off_id);
            }
        }, this);
    }
    
    if ( !composite_update_query) {
        console.log(' - (3) Update lineup query was empty (UpdateLineupSubstitutionsFunc())');
        return callback(null, "Query was empty");
    }

    db.excuteSql(composite_update_query, null, (err, result)=>{
        if (err) {
            console.log(' - (3) Update lineup error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (3) Update lineup complete : ' + result.length);
            return callback(null, result);
        }
    })
}    

/**
 * CommentariesFunc - 4
 * @param {*} commentaries 
 * @param {*} callback 
 */
function UpdateMatchStatFunc(commentaries, callback) {
    
    let update_match_stat_query =
    `
    update matches
    set localteam_stats = ?
        , visitorteam_stats = ?
        , api_comments = ?
    where match_fa_id = ?
    ;
    `

    var match_stats_parameter 
        = [ commentaries.match_stats.localteam ? JSON.stringify(commentaries.match_stats.localteam) : undefined,
            commentaries.match_stats.visitorteam ? JSON.stringify(commentaries.match_stats.visitorteam) : undefined,
            commentaries.comments ? JSON.stringify(commentaries.comments) : undefined,
            commentaries.match_id ]
    
    db.excuteSql(update_match_stat_query, match_stats_parameter, (err, result)=>{
        if (err) {
            console.log(' - (4) Update match_stats error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (4) Update match_stats complete : ' + result.affectedRows);
            return callback(null, result);
        }
    })
}

/**
 * CommentariesFunc - 5
 * @param {*} commentaries 
 * @param {*} localteam_id 
 * @param {*} visitorteam_id 
 * @param {*} callback 
 */
function UpdateLineupPlayerStatFunc(commentaries, match_data/*localteam_id, visitorteam_id*/, callback) {
    
    let composite_player_stats_update_query = ''
    let base_player_stats_update_query = 
    `
    update lineup
    set player_stats = '{0}'
    where match_fa_id = {1} and team_fa_id = {2} and player_fa_id = {3};
    `

    if (commentaries.player_stats
        && commentaries.player_stats.localteam
        && commentaries.player_stats.localteam.player ) {

            let localteam_stats = commentaries.player_stats.localteam.player
            localteam_stats.forEach(function(element) {
                
                        if ( element && element.id )  {
                            composite_player_stats_update_query 
                            += base_player_stats_update_query.format(JSON.stringify(element).replace("'", "\\'"), commentaries.match_id, match_data.localteam_id, element.id );
                        }
                    }, this);
        }

    if (commentaries.player_stats
        && commentaries.player_stats.visitorteam
        && commentaries.player_stats.visitorteam.player ) {

            let visitorteam_stats = commentaries.player_stats.visitorteam.player;
            visitorteam_stats.forEach(function(element) {
                
                if ( element && element.id )  {
                    composite_player_stats_update_query 
                    += base_player_stats_update_query.format(JSON.stringify(element).replace("'", "\\'"), commentaries.match_id, match_data.visitorteam_id, element.id );
                }
            }, this);

        }

    if ( !composite_player_stats_update_query) {
        console.log(' - (5) Update player stats query was empty');
        return callback(null, " - (5) Query was empty");
    }

    db.excuteSql(composite_player_stats_update_query, null, (err, result)=>{
        if (err) {
            console.log(' - (5) Update player stats error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (5) Update player stats complete : ' + result.length);
            return callback(null, result);
        }
    })
}


/**
 * CommentariesFunc - UpdateLineupMatchId
 * @param {*} match_id //match_fa_id
 * @param {*} callback 
 */
function UpdateLineupMatchId(match_id, callback) {
    let query =
    `
    update lineup
    set match_id = ( select match_id from matches where match_fa_id = ?)
    where match_fa_id = ?
      and match_id is null
    ;
    `

    var parameter 
        = [ match_id, match_id]
    
    db.excuteSql(query, parameter, (err, result)=>{
        if (err) {
            console.log(' - (6) Update UpdateLineupMatchId error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (6) Update UpdateLineupMatchId complete : ' + result.affectedRows);
            return callback(null, result);
        }
    })
}

function UpdateLineupPlayerId(match_id, callback) {
    let query =
    `
    update lineup l
    set player_id = (select player_id from players where player_fa_id = l.player_fa_id)
  where match_fa_id = ?
    and player_id is null;   
    ;
    `

    var parameter 
        = [ match_id]
    
    db.excuteSql(query, parameter, (err, result)=>{
        if (err) {
            console.log(' - (7) Update UpdateLineupPlayerId error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (7) Update UpdateLineupPlayerId complete : ' + result.affectedRows);
            return callback(null, result);
        }
    })
}

function UpdateLineupTeamId(match_id, callback) {
    let query =
    `
    update lineup l
    set team_id = (select team_id from teams where team_fa_id = l.team_fa_id)
  where match_fa_id = ?
    and team_id is null;
    ;
    `

    var parameter 
        = [ match_id]
    
    db.excuteSql(query, parameter, (err, result)=>{
        if (err) {
            console.log(' - (8) Update UpdateLineupTeamId error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - (8) Update UpdateLineupTeamId complete : ' + result.affectedRows);
            return callback(null, result);
        }
    })
}

