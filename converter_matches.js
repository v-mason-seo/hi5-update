const db = require('./db');
const request = require('request');
const async = require('async');

const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');

const converter_commentaries = require('./converter_commentaries')
const caster = require('./caster')
const mqtt_sender = require('./mqtt_sender')

/*--------------------------------------------------------*/


/**
 * ────────────────────────────────────────────────────────
 * 1. 매치아이디로 매치 정보를 컨버트한다.
 * ────────────────────────────────────────────────────────
 * @param {Football API 매치 아이디} match_fa_id 
 */
exports.converterMatchToId = async (match_fa_id) => {

    //------------------------------------------------------
    // Step1. Football API로 매치정보를 요청한다.
    //------------------------------------------------------
    var matchData = await asyncRequestMatchesToId(match_fa_id);

    //------------------------------------------------------
    // Step2. 매치정보 체크
    //------------------------------------------------------
    if ( matchData == null ) {
        console.log("asyncConvertMatchToId result : data null")
        return 0;
    } else if ( matchData == 'E' ) {
        console.log("asyncConvertMatchToId result : error")
        return 0;
    } else if ( matchData == 'P' ) {
        console.log("asyncConvertMatchToId result : pass")
        return 0;
    } else if ( matchData == 'L') {
        console.log("-----asyncConvertMatchToId 요청 제한으로 종료-----")
        return 0;
    }

    //------------------------------------------------------
    // Step3. 1. 매치정보 입력작업
    //        2. 매치이벤트 데이터 입력작업
    //        3. MQTT
    //------------------------------------------------------
    var matchResult = await asyncInsertMatches(matchData);
    var eventResult = await asyncInsertMatcheEvent(matchData);
    var updateResult = await asyncUpdateMatcheEvent(matchData);
    caster.sendMatchUpdated(matchResult.insertId)

    console.log("-----asyncConvertMatchToId 완료-----")
    return 1;
};

/**
 * ────────────────────────────────────────────────────────
 * 2. 킥오프 4시간 전 부터 1시간 후의 매치 정보를 업데이트 한다.
 * ────────────────────────────────────────────────────────
 */
exports.converterMatches = async function() {

    //------------------------------------------------------
    // Step1. Football API로 매치정보를 요청한다.
    //------------------------------------------------------
    var matchData = await asyncRequestMatches();

    //------------------------------------------------------
    // Step2. 매치정보 체크
    //------------------------------------------------------
    if ( matchData == null ) {
        console.log("asyncConverterMatches result : data null")
        return 0;
    } else if ( matchData == 'E' ) {
        console.log("asyncConverterMatches result : error")
        return 0;
    } else if ( matchData == 'P' ) {
        console.log("asyncConverterMatches result : pass")
        return 0;
    } else if ( matchData == 'L') {
        console.log("-----asyncConverterMatches 요청 제한으로 종료-----")
        return 0;
    }

    //------------------------------------------------------
    // Step3. 1. 매치정보 입력작업
    //        2. 매치이벤트 데이터 입력작업
    //------------------------------------------------------
    var matchResult = await asyncInsertMatches(matchData);
    var eventResult = await asyncInsertMatcheEvent(matchData);
    var updateResult = await asyncUpdateMatcheEvent(matchData);

    console.log("-----asyncConverterMatches 완료-----")
    return 1;
};


/**
 * ─────────────────────────────────────────────────────────────────────
 * 3. 시작일자와 종료일자 사이에 있는 해당 모든 매치 정보를 가져와서 컨버팅한다.
 * ─────────────────────────────────────────────────────────────────────
 * @param {시작일자} from_date 
 * @param {종료일자} to_date 
 */
exports.converterMatchTodate = async function(from_date, to_date ) {

    //------------------------------------------------------
    // Step1. Football API로 매치정보를 요청한다.
    //------------------------------------------------------
    var matchData = await asyncRequestMatchesToDate(from_date, to_date);

    //------------------------------------------------------
    // Step2. 매치정보 체크
    //------------------------------------------------------
    if ( matchData == null ) {
        console.log("asyncConverterMatches result : data null")
        return 0;
    } else if ( matchData == 'E' ) {
        console.log("asyncConverterMatches result : error")
        return 0;
    } else if ( matchData == 'P' ) {
        console.log("asyncConverterMatches result : pass")
        return 0;
    } else if ( matchData == 'L') {
        console.log("-----asyncConverterMatches 요청 제한으로 종료-----")
        return 0;
    }

    //------------------------------------------------------
    // Step3. 1. 매치정보 입력작업
    //        2. 매치이벤트 데이터 입력작업
    //------------------------------------------------------
    var matchResult = await asyncInsertMatches(matchData);
    var eventResult = await asyncInsertMatcheEvent(matchData);
    var updateResult = await asyncUpdateMatcheEvent(matchData);

    console.log("-----asyncConverterMatches 완료-----")
    return 1;
}

/**
 * 사용안함
 */
exports.converterEndMatches = () => {

    console.log('[converterEndMatches]');
    
    var now = new Date();
    var log = {};
    log.code = 'CM';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] converterEndMatches 시작합니다.";

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
            getEndMatchTaget(function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }

                return callback01(null, result);
            })
        },
        function(matchList, callback02) {

            loopMatches(matchList, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            console.log('[converterEndMatches] error : ' + err);
            log.code = 'CM';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] converterEndMatches 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
            //return 0;
        } else {
            console.log('[converterEndMatches] complete');
            log.code = 'CM';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] converterEndMatches 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
            //return 1;
        }
    })
};




/**
 * 시작일자와 종료일자 사이에 있는 해당 컴피티션 매치 정보를 가져와서 컨버팅한다.
 * @param {컴피티션 Football API Id} comp_fa_id 
 * @param {시작일자} from_date 
 * @param {종료일자} to_date 
 */
// exports.converterMatchTodate = (comp_fa_id, from_date, to_date ) => {
//     console.log('[converterMatchTodate]');

//     var now = new Date();
//     var log = {};
//     log.code = 'CM2';
//     log.subcode = dateFormat(now, "yyyymmdd HH:MM");
//     log.errcode = "history";
//     log.msg = "[시작] converterMatchTodate 시작합니다. 시작일자 : " + from_date + ", 종료일자 : " + to_date;

//     requestMatchesToDate(from_date, to_date, function (err, result ){
//         if (err) { return 0 }

//         let matches_data = result 

//         async.parallel([
//             function(callback00) {
//                 db.insertLog(log, function(err, result) {
//                     if ( err ) {
//                         return callback00(err, null);
//                     }
    
//                     return callback00(null, result);
//                 })
//             },
//             function (parallelCallback01) {
//                 // 1
//                 InsertMatches(matches_data, function(err, result) {
//                     if ( err ) {
//                         return parallelCallback01(err, null);
//                     }
//                     return parallelCallback01(null, result);
//                 })
//             },
//             function (parallelCallback02) {
//                 // 2
//                 InsertMatcheEvent(matches_data, function(err, result) {
//                     if ( err ) {
//                         return parallelCallback02(err, null);
//                     }
//                     return parallelCallback02(null, result);
//                 })
//             }
//         ], function (err, result) {
//             console.log(' - parallel complete');
//             if (err) {
//                 log.code = 'CM2';
//                 log.subcode = dateFormat(now, "yyyymmdd HH:MM");
//                 log.errcode = "history";
//                 log.msg = "[오류] converterMatchTodate 오류가 발생했습니다. 시작일자 : " + from_date + ", 종료일자 : " + to_date + "\n" + err;

//                 db.insertLog(log, function(err, result) {
//                     return 0;
//                 })
//             } else {
//                 log.code = 'CM2';
//                 log.subcode = dateFormat(now, "yyyymmdd HH:MM");
//                 log.errcode = "history";
//                 log.msg = "[종료] converterMatchTodate 시작합니다. 시작일자 : " + from_date + ", 종료일자 : " + to_date;

//                 db.insertLog(log, function(err, result) {
//                     return 1;
//                 })
//             }
//         });        
        
//     })
// }

/*--------------------------------------------------------*/

/**
 * Football API에 매치 데이터를 요청한다.
 * 
 * 파라미터를 넘기지 않으면 라이브 매치 정보를 받아온다.
 * - 라이브 경기 : When no parameters are provided, the endpoint returns live matches 
 *                (kick-off time in the last 4 hours or in the next one hour).
 */
function asyncRequestMatches() {
    
    return new Promise((resolve, reject) => {

        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : 'http://api.football-api.com/2.0/matches',
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                // error
                reject('E'); 

            } else if ( !error && response.statusCode != 200 ) {
                
                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestMatches(live) " + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestMatches(live) " + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }

            } else {
                console.log("asyncRequestMatches(live) " + " 매치정보 요청 완료")
                resolve(body);
            }
        })
    });
}

/**
 * Football API에 매치 아이디로 매치 데이터를 요청한다.
 * 
 * @param {Football API 매치 아이디} match_fa_id 
 */
function asyncRequestMatchesToId(match_fa_id) {
    
    return new Promise((resolve, reject) => {
        
        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : 'http://api.football-api.com/2.0/matches/' + match_fa_id,
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                // error
                reject('E'); 

            } else if ( !error && response.statusCode != 200 ) {
                
                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestMatchesToId " + match_fa_id + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestMatchesToId " + match_fa_id + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }

            } else {
                console.log("asyncRequestMatchesToId " + match_fa_id + " 매치정보 요청 완료")
                resolve(body);
            }
        })
    });
}


/**
 * Football API에 기간으로(from_date, to_date) 매치 데이터를 요청한다.
 */
function asyncRequestMatchesToDate (from_date, to_date) {
    
    return new Promise((resolve, reject) => {

        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : `http://api.football-api.com/2.0/matches?from_date=${from_date}&to_date=${to_date}` , 
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                // error
                reject('E'); 

            } else if ( !error && response.statusCode != 200 ) {
                
                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestMatchesToDate " + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestMatchesToDate " + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }

            } else {
                console.log("asyncRequestMatchesToDate " + " 매치정보 요청 완료")
                resolve(body);
            }
        })
    });
}


/**
 * 디비에 매치 정보를 입력한다.
 * 
 * @param {Football API에서 받은 매치정보} matchesData 
 */
function asyncInsertMatches(matchesData) {
    
    return new Promise((resolve, reject) => {

        let matches = []
        let matchesParamter = []
        var object = moment()
    
        if ( Array.isArray(JSON.parse(matchesData)) ) {
            matches = JSON.parse(matchesData);
        } else {
            matches.push(JSON.parse(matchesData));
        }
    
        matches.map ( function(match, i) {

            matchesParamter.push([matches[i].id, "" +
                                    matches[i].comp_id, "" +
                                    moment(matches[i].formatted_date + ' ' + matches[i].time, 'DD.MM.YYYY HH:mm').format('YYYY-MM-DD HH:mm'), "" +
                                    moment.utc(matches[i].formatted_date + ' ' + matches[i].time, 'DD.MM.YYYY HH:mm').tz("Asia/Seoul").format('YYYY-MM-DD HH:mm'), "" +
                                    matches[i].season, "" +
                                    matches[i].week.length > 25 ? matches[i].week.substring(1, 24) : matches[i].week, "" +
                                    matches[i].venue, "" +
                                    matches[i].venue_id ? matches[i].venue_id : undefined, "" +
                                    matches[i].venue_city, "" +
                                    matches[i].status, "" +
                                    matches[i].timer ? matches[i].timer : undefined, "" +
                                    matches[i].time != 'Postp.' ? matches[i].time : undefined, "" +
                                    matches[i].localteam_id, "" +
                                    matches[i].localteam_name, "" +
                                    matches[i].localteam_score ? parseInt(matches[i].localteam_score) || 0 : undefined, "" +
                                    matches[i].visitorteam_id, "" +
                                    matches[i].visitorteam_name, "" +
                                    matches[i].visitorteam_score ? parseInt(matches[i].visitorteam_score) || 0 : undefined, "" +
                                    matches[i].ht_score ? matches[i].ht_score : undefined, "" +
                                    matches[i].ft_score ? matches[i].ft_score : undefined, "" +
                                    matches[i].et_score ? matches[i].et_score : undefined, "" +
                                    matches[i].penalty_local ? matches[i].penalty_local : undefined, "" +
                                    matches[i].penalty_visitor ? matches[i].penalty_visitor : undefined
                                ]);
        })
    
        let query =
        `
        INSERT INTO matches (  match_fa_id,
                                comp_fa_id, 
                                match_date_utc,
                                match_date,
                                season,
                                week,
                                venue,
                                venue_id,
                                venue_city,
                                status,
                                timer,
                                time,
                                localteam_id,
                                localteam_name,
                                localteam_score,
                                visitorteam_id,
                                visitorteam_name,
                                visitorteam_score,
                                ht_score,
                                ft_score,
                                et_score,
                                penalty_local,
                                penalty_visitor) 
                                VALUES ? 
            ON DUPLICATE KEY UPDATE 
            match_date_utc    = VALUES(match_date_utc),
            match_date        = VALUES(match_date),
            season            = VALUES(season),
            week              = VALUES(week),
            venue             = VALUES(venue),
            venue_id          = VALUES(venue_id),
            venue_city        = VALUES(venue_city),
            status            = VALUES(status),
            timer             = VALUES(timer),
            time              = VALUES(time),
            localteam_id      = VALUES(localteam_id),
            localteam_name    = VALUES(localteam_name),
            localteam_score   = VALUES(localteam_score),
            visitorteam_id    = VALUES(visitorteam_id),
            visitorteam_name  = VALUES(visitorteam_name),
            visitorteam_score = VALUES(visitorteam_score),
            ht_score          = VALUES(ht_score),
            ft_score          = VALUES(ft_score),
            et_score          = VALUES(et_score),
            penalty_local     = VALUES(penalty_local),
            penalty_visitor   = VALUES(penalty_visitor),
            updated           = now()
            ;
        `
    
        db.excuteSql(query, [matchesParamter], (err, result)=>{
            if (err) {
                console.log(' - insert matches error : ' + err);

                var now = new Date();
                var log = {};
                log.code = 'CM';
                log.subcode = dateFormat(now, "yyyymmdd HH:MM");
                log.errcode = "history";
                log.msg = "[asyncInsertMatches] - " + err;
                return reject(err)
            } else {
                console.log(' - insert matches complete : ' + result.info);
                return resolve(result)
            }
        })        
    });
}



function asyncInsertMatcheEvent(matchesData) {

    return new Promise((resolve, reject) => {

        let matches = []
        let match_events = [];
    
        if ( Array.isArray(JSON.parse(matchesData)) ) {
            matches = JSON.parse(matchesData);
        } else {
            matches.push(JSON.parse(matchesData));
        }

        if ( matches.length == 0) {
            return resolve('P')
        }
        
        matches.forEach(function(match) {
            let match_fa_id = match.id
            match.events.forEach(function(event) {
                event.match_id = match_fa_id;
                match_events.push(event);
            }, this);
        }, this);

        if ( match_events.length == 0) {
            return resolve('P')
        }
    
        let eventsParameter = [];
    
        match_events.map( function(event, i) {
            eventsParameter.push([event.id, "" +
                                    event.type, "" +
                                    event.minute ? event.minute : undefined, "" +
                                    event.extra_min ? event.extra_min : undefined, "" +
                                    event.team, "" +
                                    event.player, "" +
                                    event.player_id ? event.player_id : undefined, "" +
                                    event.assist, "" +
                                    event.assist_id ? event.assist_id : undefined, "" +
                                    event.result, "" +
                                    event.match_id
                                ]);
        })
    
        let event_query =
        `
        insert into match_events(match_event_fa_id,
                                type,
                                minute,
                                extra_min,
                                team,
                                player_name,
                                player_fa_id,
                                assist,
                                assist_fa_id,
                                result,
                                match_fa_id) VALUES ?
        ON DUPLICATE KEY UPDATE 
        type         = VALUES(type),
        minute       = VALUES(minute),
        extra_min    = VALUES(extra_min),
        team         = VALUES(team),
        player_name  = VALUES(player_name),
        player_fa_id = VALUES(player_fa_id),
        assist       = VALUES(assist),
        assist_fa_id = VALUES(assist_fa_id),
        result       = VALUES(result),
        updated      = now();
        `
    
        db.excuteSql(event_query, [eventsParameter], (err, result)=>{
            if (err) {
                console.log(' - insert match_events error : ' + err);
                return reject(err)
            } else {
                console.log(' - insert match_events complete : ' + result.info);
                return resolve(result)
            }
        })        
    });
}


function asyncUpdateMatcheEvent(matchesData) {

    return new Promise((resolve, reject) => {

        let composite_update_query = '';
        let matches = []
        let match_events = [];

        if ( Array.isArray(JSON.parse(matchesData)) ) {
            matches = JSON.parse(matchesData);
        } else {
            matches.push(JSON.parse(matchesData));
        }

        // 이벤트 데이터가 없는 경우.
        // if ( match_events.length == 0 ) {
        //     return callback(null, 'event data empty');
        // }

        let event_query =
        `
        update match_events a
        set    a.match_id = ( select max(x.match_id) from matches x where x.match_fa_id = {0})
        where  a.match_fa_id = {1};
        `

        matches.forEach(function(match) {
            composite_update_query += event_query.format(match.id, match.id);
        }, this);
    
        db.excuteSql(composite_update_query, null, (err, result)=>{
            if (err) {
                console.log(' - asyncUpdateMatcheEvent error : ' + err);
                return reject(err)
            } else {
                console.log(' - asyncUpdateMatcheEvent complete : ' + result.info);
                return resolve(result)
            }
        })        
    });
}



/*--------------------------------------------------------*/

function loopMatches(match_list, callback) {

    let index = 0;

    async.whilst (
        function(callback01) {
            return index < match_list.length;
        },
        function(callback02) {

            let match_fa_id = match_list[index].match_fa_id;
            console.log(' - [loopMatches] ' + (index + 1) + ' / ' + match_list.length + ', match fa id : ' + match_fa_id);
            index++;

            request_insert_matches(match_fa_id, function(err, result) {
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
                return callback(null, match_list);
            } 
        }
    )
}

function InsertMatchData(matches_data, callback ){
    async.parallel([
        function (parallelCallback01) {
            // 1
            InsertMatches(matches_data, function(err, result) {
                if ( err ) {
                    return parallelCallback01(err, null);
                }
                return parallelCallback01(null, result);
            })
        },
        function (parallelCallback02) {
            // 2
            InsertMatcheEvent(matches_data, function(err, result) {
                if ( err ) {
                    return parallelCallback02(err, null);
                }
                return parallelCallback02(null, result);
            })
        }
    ], function (err, result) {
        console.log(' - parallel complete');
        if (err) {
            return 0;
        }

        return 1;
    });
}

function request_insert_matches(match_fa_id, callback) {

    var tasks = [
        function(callback01) {
            requestMatchesToId(match_fa_id, function(err, result) { 
                if ( err ) {
                    return callback01(err, null);
                }
                return callback01(null, result);
            } )
        },
        function(matches_data, callback02) {

            if ( matches_data == 'next' ) {
                return callback02(null, matches_data);
            }

            async.parallel([
                function (parallelCallback01) {
                    // 1
                    InsertMatches(matches_data, function(err, result) {
                        if ( err ) {
                            return parallelCallback01(err, null);
                        }
                        return parallelCallback01(null, result);
                    })
                },
                function (parallelCallback02) {
                    // 2
                    InsertMatcheEvent(matches_data, function(err, result) {
                        if ( err ) {
                            return parallelCallback02(err, null);
                        }
                        return parallelCallback02(null, result);
                    })
                }
            ], function (err, result) {
                console.log(' - parallel complete');
                if (err) {
                    return callback02(err,null);
                }
        
                return callback02(null, result);
            });

            // InsertMatches(matches_data, function(err, result) {
            //     if ( err ) {
            //         return callback02(err, null);
            //     }
            //     return callback02(null, result);
            // })
        }
    ];

    async.waterfall(tasks, function(err, result) {

        if ( err ) {
            return callback(err, null);    
        }

        return callback(null, result);;
        
    })
}

function getEndMatchTaget(callback) {

    console.log(' - [getEndMatchTaget]');

    let query = 
    `
    select a.match_fa_id
        ,  a.match_id
        ,  ht.team_id home_team_id
        ,  a.visitorteam_id
        ,  at.team_id away_team_id
    from matches a
         left join teams ht on a.localteam_id = ht.team_fa_id
         left join teams at on a.visitorteam_id = at.team_fa_id
    where a.match_date between date_sub(now(), interval 3 day) 
                    and date_add(now(), interval 90 minute)
    #and updated < date_add(match_date, interval 90 minute)
    and a.status not in ('FT', 'Postp.', 'Pen.', 'AET', 'Aban.')
    order by a.match_date desc;
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
 * 파라미터를 넘기지 않으면 라이브 매치 정보를 받아온다.
 * - 라이브 경기 : When no parameters are provided, the endpoint returns live matches 
 *   (kick-off time in the last 4 hours or in the next one hour).
 * @param {*} callback 
 */
function requestMatches(callback) {
    
    console.log(' - [requestMatches]');

    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : 'http://api.football-api.com/2.0/matches',
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            // var log = {};
            // log.code = 'CM';
            // log.subcode = match_fa_id;
            // log.errcode = JSON.parse(body).code;
            // log.msg = JSON.parse(body).message;

            // db.insertLog(log, function(err, result) {
            //     return callback(null, 'next');
            // })

            return callback(null, 'next');
        } else {
            return callback(null, body);
        }
    })
}


function requestMatchesToId(match_fa_id, callback) {
    
    console.log(' - [requestMatchesToId]');

    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : 'http://api.football-api.com/2.0/matches/' + match_fa_id,
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            var log = {};
            log.code = 'CM';
            log.subcode = match_fa_id;
            log.errcode = JSON.parse(body).code;
            log.msg = JSON.parse(body).message;

            db.insertLog(log, function(err, result) {
                return callback(null, 'next');
            })
        } else {
            return callback(null, body);
        }
    })
}

//http://api.football-api.com/2.0/matches?from_date=2018-1-1&to_date=2018-6-30&Authorization=565ec012251f932ea4000001fa542ae9d994470e73fdb314a8a56d76
function requestMatchesToDate(from_date,to_date, callback) {
    
    console.log(' - [requestMatchesToDate]');

    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : `http://api.football-api.com/2.0/matches?from_date=${from_date}&to_date=${to_date}` , 
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            var log = {};
            log.code = 'CM'; 
            log.subcode = ''; //
            log.errcode = JSON.parse(body).code;
            log.msg = '[' + from_date + ',' + to_date + ']' + JSON.parse(body).message;

            db.insertLog(log, function(err, result) {
                return callback('err', null);
            })
        } else {
            return callback(null, body);
        }
    })
}

function InsertMatches(matchesData, callback) {
    
    //let matches = JSON.parse(matchesData);
    let matches = []
    let matchesParamter = []
    var object = moment()

    if ( Array.isArray(JSON.parse(matchesData)) ) {
        matches = JSON.parse(matchesData);
    } else {
        matches.push(JSON.parse(matchesData));
    }

    matches.map ( function(match, i) {
        matchesParamter.push([matches[i].id, "" +
                                matches[i].comp_id, "" +
                                moment(matches[i].formatted_date + ' ' + matches[i].time, 'DD.MM.YYYY HH:mm').format('YYYY-MM-DD HH:mm'), "" +
                                moment.utc(matches[i].formatted_date + ' ' + matches[i].time, 'DD.MM.YYYY HH:mm').tz("Asia/Seoul").format('YYYY-MM-DD HH:mm'), "" +
                                //moment.tz(matches[i].formatted_date + ' ' + matches[i].time, 'DD.MM.YYYY HH:mm', "Europe/London").tz("Asia/Seoul").format('YYYY-MM-DD HH:mm'), "" +
                                matches[i].season, "" +
                                //matches[i].week ? parseInt(matches[i].week) || 0 : undefined, "" +
                                matches[i].week, "" +
                                matches[i].venue, "" +
                                matches[i].venue_id ? matches[i].venue_id : undefined, "" +
                                matches[i].venue_city, "" +
                                matches[i].status, "" +
                                matches[i].timer ? matches[i].timer : undefined, "" +
                                matches[i].time != 'Postp.' ? matches[i].time : undefined, "" +
                                matches[i].localteam_id, "" +
                                matches[i].localteam_name, "" +
                                matches[i].localteam_score ? parseInt(matches[i].localteam_score) || 0 : undefined, "" +
                                matches[i].visitorteam_id, "" +
                                matches[i].visitorteam_name, "" +
                                matches[i].visitorteam_score ? parseInt(matches[i].visitorteam_score) || 0 : undefined, "" +
                                matches[i].ht_score ? matches[i].ht_score : undefined, "" +
                                matches[i].ft_score ? matches[i].ft_score : undefined, "" +
                                matches[i].et_score ? matches[i].et_score : undefined, "" +
                                matches[i].penalty_local ? matches[i].penalty_local : undefined, "" +
                                matches[i].penalty_visitor ? matches[i].penalty_visitor : undefined
                            ]);
    })

    let query =
    `
    INSERT INTO matches (  match_fa_id,
                            comp_fa_id, 
                            match_date_utc,
                            match_date,
                            season,
                            week,
                            venue,
                            venue_id,
                            venue_city,
                            status,
                            timer,
                            time,
                            localteam_id,
                            localteam_name,
                            localteam_score,
                            visitorteam_id,
                            visitorteam_name,
                            visitorteam_score,
                            ht_score,
                            ft_score,
                            et_score,
                            penalty_local,
                            penalty_visitor) 
                            VALUES ? 
        ON DUPLICATE KEY UPDATE 
        match_date_utc    = VALUES(match_date_utc),
        match_date        = VALUES(match_date),
        season            = VALUES(season),
        week              = VALUES(week),
        venue             = VALUES(venue),
        venue_id          = VALUES(venue_id),
        venue_city        = VALUES(venue_city),
        status            = VALUES(status),
        timer             = VALUES(timer),
        time              = VALUES(time),
        localteam_id      = VALUES(localteam_id),
        localteam_name    = VALUES(localteam_name),
        localteam_score   = VALUES(localteam_score),
        visitorteam_id    = VALUES(visitorteam_id),
        visitorteam_name  = VALUES(visitorteam_name),
        visitorteam_score = VALUES(visitorteam_score),
        ht_score          = VALUES(ht_score),
        ft_score          = VALUES(ft_score),
        et_score          = VALUES(et_score),
        penalty_local     = VALUES(penalty_local),
        penalty_visitor   = VALUES(penalty_visitor),
        updated           = now()
        ;
    `

    db.excuteSql(query, [matchesParamter], (err, result)=>{
        if (err) {
            console.log(' - insert matches error : ' + err);
            callback(err, null);
        } else {
            console.log(' - insert matches complete : ' + result.info);
            callback(null, result);
        }
    })
}


function InsertMatcheEvent(matchesData, callback) {

    let matches = []
    let match_events = [];

    if ( Array.isArray(JSON.parse(matchesData)) ) {
        matches = JSON.parse(matchesData);
    } else {
        matches.push(JSON.parse(matchesData));
    }
    
    matches.forEach(function(match) {
        let match_id = match.id
        match.events.forEach(function(event) {
            event.match_id = match_id;
            match_events.push(event);
        }, this);
    }, this);

    // 이벤트 데이터가 없는 경우.
    if ( match_events.length == 0 ) {
        console.log(' - insert match_events error : empty data');
        return callback(null, 'event data empty');
    }

    let eventsParameter = [];

    match_events.map( function(event, i) {
        eventsParameter.push([event.id, "" +
                                event.type, "" +
                                event.minute ? event.minute : undefined, "" +
                                event.extra_min ? event.extra_min : undefined, "" +
                                event.team, "" +
                                event.player, "" +
                                event.player_id ? event.player_id : undefined, "" +
                                event.assist, "" +
                                event.assist_id ? event.assist_id : undefined, "" +
                                event.result, "" +
                                event.match_id
                            ]);
    })

    let event_query =
    `
    insert into match_events(match_event_fa_id,
                            type,
                            minute,
                            extra_min,
                            team,
                            player_name,
                            player_fa_id,
                            assist,
                            assist_fa_id,
                            result,
                            match_fa_id) VALUES ?
    ON DUPLICATE KEY UPDATE 
    type         = VALUES(type),
    minute       = VALUES(minute),
    extra_min    = VALUES(extra_min),
    team         = VALUES(team),
    player_name  = VALUES(player_name),
    player_fa_id = VALUES(player_fa_id),
    assist       = VALUES(assist),
    assist_fa_id = VALUES(assist_fa_id),
    result       = VALUES(result),
    updated      = now();
    `

    db.excuteSql(event_query, [eventsParameter], (err, result)=>{
        if (err) {
            console.log(' - insert match_events error : ' + err);
            callback(err, null);
        } else {
            console.log(' - insert match_events complete : ' + result.info);
            callback(null, result);
        }
    })
}