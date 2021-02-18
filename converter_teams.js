const db = require('./db');
const request = require('request');
const async = require('async');

const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');

/*-------------------------------------------------------------*/



/** 
 * 이적시기때마다 돌려주면 됨..
 * 팀 정보 업데이트 먼저하고 돌려야 함..
*/
exports.updatePlayerTeamSquad = () => {
    
    console.log('[convertTeamToId]');

    let query =
    `
    select p.player_fa_id
        ,  ifnull(p.player_name_kor, p.player_name) as player_name
        ,  p.teamid            as before_team_fa_id
        ,  p.team              as before_team_name
        ,  t.after_team_fa_id  as after_team_fa_id
        ,  t.after_team_name   as after_team_name
    from
    (    
        select idx
            , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].id'))) as player_id
            , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].name'))) as player_name
            , t.team_fa_id as after_team_fa_id
            , ifnull(t.team_name_kor, t.team_name) as after_team_name
        from teams t
        join 
        (
            select 0 as idx union select 1 as idx union select 2 as idx union select 3 as idx union select 4 as idx union select 5 as idx union
            select 6 as idx union select 7 as idx union select 8 as idx union select 9 as idx union select 10 as idx union select 11 as idx union 
            select 12 as idx union select 13 as idx union select 14 as idx union select 15 as idx union select 16 as idx union select 17 as idx union
            select 18 as idx union select 19 as idx union select 20 as idx union select 21 as idx union select 22 as idx union select 23 as idx union
            select 24 as idx union select 25 as idx union select 26 as idx union select 27 as idx union select 28 as idx union select 29 as idx union 
            select 30 as idx union select 31 as idx union select 32 as idx union select 33 as idx union select 34 as idx union select 35 as idx union
            select 36 as idx union select 37 as idx union select 38 as idx union select 39 as idx union select 40 as idx union select 41 as idx union
            select 42 as idx union select 43 as idx union select 44 as idx union select 44 as idx union select 45 as idx union select 46 as idx 
        ) as indexes
    ) t, players p
    where t.player_id = p.player_fa_id
    and t.after_team_fa_id != p.teamid
    #and t.player_id = 102559
    ;               
    `

    db.excuteSql(query, null, function (err, result){
        
        if (err) {
            return 0;
        }
        
        let list = result;
        let updateQuery =
        `
        update players p
        set    p.teamid = ?
            ,  p.team   = ?
        where  p.player_fa_id = ?
        ;
        `


        list.forEach(item => {
            console.log('player_fa_id: ' + item.player_fa_id + ', player_name: ' + item.player_name 
            + ', team_id(b) : ' + item.before_team_fa_id + ", team_name(b) : " + item.before_team_name
            + ', team_id(a) : ' + item.after_team_fa_id + ", team_name(a) : " + item.after_team_name );
            
            var parameter = [item.after_team_fa_id, item.after_team_name, item.player_fa_id];

            db.excuteSql(updateQuery, parameter, function (err, result) {
        
                if (err) {
                    console.log('[error] player_id : ' + item.player_id + ', error message : ' + err);
                } else {
                    console.log('[compete] player_id : ' + item.player_id);
                }
            });
        });
    });
};



exports.convertTeamToId = (team_fa_id) => {
    
    console.log('[convertTeamToId]');

    var now = new Date();
    var log = {};
    log.code = 'CT2';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] convertTeamToId 시작합니다.";

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
            request_insert_team(team_fa_id, function(err, result){
                if (err) {
                    return callback01(err, null);
                } else {
                    return callback01(null, result);
                } 
            })
        }
    ];
    
    async.waterfall(tasks, function (err, result) {
        if (err) {
            console.log('[convertTeamToId] error : ' + err);
            log.code = 'CT2';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] convertTeamToId 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
        }
        else {
            console.log('[convertTeamToId] complete');
            log.code = 'CT2';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] convertTeamToId 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
        }
    });
};


exports.convertTeamsNotExists = function() {
    
    console.log('[convertTeamsNotExists]');

    var now = new Date();
    var log = {};
    log.code = 'CT';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] convertTeamsNotExists 시작합니다.";

    var tasks = [
        function(callback00) {
            db.insertLog(log, function(err, result) {
                if ( err ) {
                    return callback00(err);
                }

                return callback00(null);
            })
        },
        function (callback01) {
            let query = 
            `
            select team_fa_id
            from
            (
                select a.localteam_id team_fa_id
                from   matches a
                    ,  competitions c
                where  a.comp_fa_id = c.comp_fa_id
                #and    ifnull(c.is_active, 0) = 1
                and    ifnull(a.localteam_id, 0) != 0
                and    not exists ( select 'x' from teams b where localteam_id = b.team_fa_id)
                union all
                select a.visitorteam_id team_fa_id
                from   matches a
                    ,  competitions c
                where  a.comp_fa_id = c.comp_fa_id
                #and    ifnull(c.is_active, 0) = 1
                and    ifnull(a.visitorteam_id, 0) != 0
                and    not exists ( select 'x' from teams b where a.visitorteam_id = b.team_fa_id)
            ) a
            group by team_fa_id
            order by team_fa_id
            ;
            `
            // `
            // select p.teamid team_fa_id
            // from   players p
            // where  not exists ( select 'x' from teams t where p.teamid = t.team_fa_id)
            // and    p.teamid is not null
            // group by p.teamid
            // ;
            // `
            
            db.excuteSql(query, null, function (err, result){
                
                if (err) {
                    return callback01(err, null);
                } else {
                    return callback01(null, result);
                }
            });
        },
        function (team_list, callback02) {
            
            loopTeams(team_list, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];
    
    async.waterfall(tasks, function (err, result) {
        if (err) {
            console.log('[convertTeamsNotExists] error : ' + err);
            log.code = 'CT';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] convertTeamsNotExists 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
        }
        else {
            console.log('[convertTeamsNotExists] complete');
            log.code = 'CT';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] convertTeamsNotExists 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
        }
    });
};


exports.convertAllTeams = () => {
    
    console.log('[convertAllTeams]');

    var now = new Date();
    var log = {};
    log.code = 'CT';
    log.subcode = dateFormat(now, "yyyymmdd HH:MM");
    log.errcode = "history";
    log.msg = "[시작] convertAllTeams 시작합니다.";

    var tasks = [
        function(callback00) {
            db.insertLog(log, function(err, result) {
                if ( err ) {
                    return callback00(err);
                }

                return callback00(null);
            })
        },
        function (callback01) {
            getAllTeams(function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }
                return callback01(null, result);
            })
        },
        function (team_list, callback02) {
            
            loopTeams(team_list, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
            })
        }
    ];
    
    async.waterfall(tasks, function (err, result) {
        if (err) {
            console.log('[convertAllTeams] error : ' + err);
            log.code = 'CT';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[오류] convertAllTeams 오류가 발생했습니다.\n" + err;

            db.insertLog(log, function(err, result) {
                return 0;
            })
        }
        else {
            console.log('[convertAllTeams] complete');
            log.code = 'CT';
            log.subcode = dateFormat(now, "yyyymmdd HH:MM");
            log.errcode = "history";
            log.msg = "[종료] convertAllTeams 종료합니다.";

            db.insertLog(log, function(err, result) {
                return 1;
            })
        }
    });
};

/*-------------------------------------------------------------*/

function loopTeams(team_list, callback) {
    
    let index = 0;

    async.whilst(
        function(callback) {
            return index < team_list.length;
        },
        function(callback) {

            let team_id = team_list[index].team_fa_id;
            console.log(' - [loopTeams] ' + (index+1) + ' / ' + team_list.length + ', team id : ' + team_id);
            index++;

            request_insert_team(team_id, function(err, result){
                if (err) {
                    return callback(err, null);
                } else {
                    return callback(null, result);
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
    );
};

function request_insert_team(team_id, callback) {
    
    var tasks = [
        function (callback01) {
            // 1. 팀 데이터 요청
            requestTeam(team_id, function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }
                return callback01(null, result);
            })
        },
        function (team_data, callback02) {
            
            if ( team_data == 'next' ) {
                console.log(' - [request_insert_team] team data empty'); 
                return callback02(null, team_data);
            }

            // 2. 팀 데이터 입력
            insertTeam(team_data, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, team_data);
            })
        },
        function (team_data, callback03) {
            
            if ( team_data == 'next' ) {
                console.log(' - [request_insert_team] team data empty'); 
                return callback03(null, team_data);
            }

            // 2. 팀 데이터 입력
            insertTransfer(team_data, function(err, result) {
                if ( err ) {
                    return callback03(err, null);
                }
                return callback03(null, 15692);
            })
        },
        function(team_fa_id, callback04) {
            
            if ( !team_fa_id) {
                console.log(' - [updateTransferPlayer] team_fa_id null!'); 
                return callback04(null, team_fa_id);
            }

            updateTransferPlayer(team_fa_id, function(err, result) {
                if ( err ) {
                    return callback04(err, null);
                }
                return callback04(null, result);
            })
        }
    ];
    
    async.waterfall(tasks, function (err, result) {
        if (err) {
            console.log('err');
            callback(err, null);
        } else {
            callback(null, result);
        }
    });
};

function requestTeam(team_id, callback) {
    
    console.log(' - [requestTeam]');

    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : 'http://api.football-api.com/2.0/team/' + team_id,
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            var log = {};
            log.code = 'CT';
            log.subcode = team_id;
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

function insertTeam(team_data, callback) {
    
    if ( team_data == 'next' ) {
        console.log(' - [insertTeam] data empty');    
        return callback(null, 'next');
    }

    console.log(' - [insertTeam]');

    let team = JSON.parse(team_data);
    let teamParamter = []

    
    teamParamter.push([team.team_id,
            team.is_national == 'True' ? 1 : 0,
            team.name,
            team.country,
            team.founded,
            team.leagues,
            team.venue_name,
            team.venue_id,
            team.venue_surface,
            team.venue_address,
            team.venue_city,
            Number.isInteger(team.venue_capacity) ? team.venue_capacity : undefined,
            team.coach_name,
            team.coach_id ? team.coach_id : undefined,
            team.squad ? JSON.stringify(team.squad) : undefined,
            team.sidelined ? JSON.stringify(team.sidelined) : undefined,
            team.transfers_in ? JSON.stringify(team.transfers_in) : undefined,
            team.transfers_out ? JSON.stringify(team.transfers_out) : undefined,
            team.statistics ? JSON.stringify(team.statistics) : undefined
            ]);

    let query =
    `
    INSERT INTO teams (team_fa_id,
                    is_national,
                    team_name,
                    country,
                    founded,
                    leagues,
                    venue_name,
                    venue_id,
                    venue_surface,
                    venue_address,
                    venue_city,
                    venue_capacity,
                    coach_name,
                    coach_id,
                    squad,
                    sidelined,
                    transfers_in,
                    transfers_out,
                    statistics) 
                VALUES ? 
    ON DUPLICATE KEY UPDATE 
    venue_name     = VALUES(venue_name),
    venue_id       = VALUES(venue_id),
    venue_surface  = VALUES(venue_surface),
    venue_address  = VALUES(venue_address),
    venue_id       = VALUES(venue_id),
    venue_city     = VALUES(venue_city),
    venue_capacity = VALUES(venue_capacity),
    coach_name     = VALUES(coach_name),
    coach_id       = VALUES(coach_id),
    squad          = VALUES(squad),
    sidelined      = VALUES(sidelined),
    transfers_in   = VALUES(transfers_in),
    transfers_out  = VALUES(transfers_out),
    statistics     = VALUES(statistics),
    updated        = now(),
    mentioned      = mentioned,
    hits           = hits,
    tag            = tag,
    priority       = priority,
    best_players   = best_players,
    worst_players  = worst_players,
    overview       = overview
    ;
    `

    db.excuteSql(query, [teamParamter], (err, result)=>{
        if (err) {
            console.log(' - [insertTeam] error : ' + err);

            var log = {};
            log.code = 'CT';
            log.subcode = team.id;
            log.errcode = err.errno;
            log.msg = err.message;
            db.insertLog(log, function(err, result){
                callback(null, null);
            })
            
        } else {
            callback(null, result);
        }
    })
}

function updateTransferPlayer(team_fa_id, callback) {

    let query =
    `
    select a.gb, a.player_fa_id, a.player_name, a.date, a.from_team_id, a.from_team_name, a.to_team_fa_id, a.to_team_name, b.teamid, b.team, t.is_national
    from   transfers a
         , players b
         , teams t
    where  a.player_fa_id = b.player_fa_id
    and    a.to_team_fa_id != b.teamid
    and    (     ( a.gb = 'out' and a.from_team_fa_id = ? and t.team_fa_id = a.from_team_fa_id )
              or ( a.gb = 'in' and a.to_team_fa_id = ? and t.team_fa_id = a.to_team_fa_id ) )
    and     a.date = ( select max(x.date)
                       from   transfers x
                       where  a.player_fa_id = x.player_fa_id
                       and    a.gb = x.gb )
    order by gb, player_fa_id               
    `

    var parameter = [team_fa_id, team_fa_id];

    

    var tasks = [
        function(callback01) {
            db.excuteSql(query, parameter, function (err, result){
        
                if (err) {
                    return callback01(err, null);
                } else {
                    return callback01(null, result);
                }
            });
        },
        function(trans_list, callback02) {
            
            let index = 0;

            async.whilst (
                function(callback03) {
                    return index < trans_list.length;
                },
                function(callback04) {
                    console.log('[Transfer] ' + (index + 1) + ' / ' + trans_list.length);
                    
                    //return callback04(null, trans_list);
                    let updateQuery

                    if ( trans_list.is_national == 0) {
                        updateQuery = 
                        `
                        update players a
                        set    a.teamid = ?
                             , a.team   = ? 
                        where a.player_fa_id = ? 
                        `
                    } else {
                        updateQuery = 
                        `
                        update players a
                        set    a.national_teamid = ?
                             , a.nationality   = ? 
                        where a.player_fa_id = ? 
                        `
                    }

                    var updateParameter = [ trans_list[index].to_team_fa_id, trans_list[index].to_team_name, trans_list[index].player_fa_id ];
                    index++;

                    db.excuteSql(updateQuery, updateParameter, function (err, result){
        
                        if (err) {
                            return callback04(err, null);
                        } else {
                            return callback04(null, result);
                        }
                    });
                },
                function(err, result) {
                    if (err) {
                        return callback02(err, null);
                    } else {
                        return callback02(null, result);
                    } 
                }
            );
        }
    ]


    async.waterfall(tasks, function(err, result) {
        if ( err ) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}

function insertTransfer(team_data, callback) {
    
    if ( team_data == 'next' ) {
        console.log(' - [insertTransfer] data empty');    
        return callback(null, 'next');
    }

    console.log(' - [insertTransfer]');

    let team = JSON.parse(team_data);
    let transfers_in = team.transfers_in;
    let transfers_out = team.transfers_out;
    let teamParamter = []
    
    transfers_in.forEach(function(trans_in) {
            
        teamParamter.push([
            'in',
            trans_in.type,
            moment(trans_in.date, 'DD.MM.YY').format('YYYY-MM-DD'),
            trans_in.id,
            trans_in.id,
            trans_in.name,
            trans_in.team_id,  // from
            trans_in.team_id,
            trans_in.from_team,
            team.team_id,  // to
            team.team_id,
            team.name
        ]);

    }, this);

    transfers_out.forEach(function(trans_out) {
            
        teamParamter.push([
            'out',
            trans_out.type,
            moment(trans_out.date, 'DD.MM.YY').format('YYYY-MM-DD'),
            trans_out.id,
            trans_out.id,
            trans_out.name,
            team.team_id,  // from
            team.team_id,
            team.name,
            trans_out.team_id,  // to
            trans_out.team_id,
            trans_out.to_team
        ]);

    }, this);

    let query =
    `
    INSERT INTO transfers
     (  gb,
        type,
        date,
        player_id,
        player_fa_id,
        player_name,
        from_team_id,
        from_team_fa_id,
        from_team_name,
        to_team_id,
        to_team_fa_id,
        to_team_name ) 
    VALUES ? 
    ON DUPLICATE KEY UPDATE 
        gb              = VALUES(gb),
        type            = VALUES(type),
        date            = VALUES(date),
        player_id       = VALUES(player_id),
        player_fa_id    = VALUES(player_fa_id),
        player_name     = VALUES(player_name),
        from_team_id    = VALUES(from_team_id),
        from_team_fa_id = VALUES(from_team_fa_id),
        from_team_name  = VALUES(from_team_name),
        to_team_id      = VALUES(to_team_id),
        to_team_fa_id   = VALUES(to_team_fa_id),
        to_team_name    = VALUES(to_team_name)
    ;
    `

    db.excuteSql(query, [teamParamter], (err, result)=>{
        if (err) {
            console.log(' - [insertTransfer] error : ' + err);

            var log = {};
            log.code = 'CT';
            log.subcode = team.id;
            log.errcode = err.errno;
            log.msg = err.message;
            db.insertLog(log, function(err, result){
                callback(null, null);
            })
            
        } else {
            callback(null, result);
        }
    })
}

function getAllTeams(callback) {
    
    console.log(' - [getAllTeams]');

    let query = 
    // `
    // select distinct team_fa_id
    // from   teams a
    // where  is_national = 1
    // order by a.team_fa_id
    // ;
    // `
    // `
    // select distinct team_fa_id
    // from   standings
    // where  comp_fa_id = 1204
    // `
    `
    select distinct team_fa_id
    from   competitions a
        ,  standings b
    where  a.is_active is not null
    and    a.priority > 2
    and    a.comp_fa_id = b.comp_fa_id
    and    a.comp_fa_id != 1204
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

function getTeamsNotExists(callback) {
    
    console.log(' - [getTeamsNotExists]');

    let query = 
    `
    select distinct team_fa_id
    from   lineup a
    where not exists ( select 'x' from teams b where a.team_fa_id = b.team_fa_id)
    order by a.team_fa_id
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

function getTeamsNotExists2(callback) {
    
    console.log(' - [getTeamsNotExists]');

    let query = 
    `
    select team_fa_id
    from
    (
        select a.localteam_id team_fa_id
        from   matches a
        where  a.comp_fa_id not in (1204, 1269, 1399, 1221, 1229)
        and    not exists ( select 'x' from teams b where localteam_id = b.team_fa_id)
        union all
        select a.visitorteam_id team_fa_id
        from   matches a
        where  a.comp_fa_id not in (1204, 1269, 1399, 1221, 1229)
        and    not exists ( select 'x' from teams b where a.visitorteam_id = b.team_fa_id)
    ) a
    group by team_fa_id
    order by team_fa_id
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