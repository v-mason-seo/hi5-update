const db = require('./db');
const request = require('request');
const async = require('async');
const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');

/*-------------------------------------------------------------*/


/**
 * ────────────────────────────────────────────────────────
 * 0. SQL로 플레이어 데이터를 컨버트한다.
 * ────────────────────────────────────────────────────────
 * @param {Football API 플레이어 아이디} player_fa_id 
 */
exports.converterPlayerbySql = async function(query) {

    var playerList = await getQuery(query);

    if ( playerList == null || playerList.length == 0 ) {
        console.log(' 선수 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < playerList.length; i++) {

        var player = playerList[i];

        console.log(i+1 + " / " + playerList.length)

        //------------------------------------------------------
        // Step2. Football API로 선수정보를 요청한다.
        //------------------------------------------------------
        var playerData = await asyncRequestPlayer(player.player_fa_id)

        console.log('1')

        //------------------------------------------------------
        // Step3. 선수정보 입력작업
        //------------------------------------------------------
        if ( playerData == null ) {
            console.log("asyncRequestPlayer result : data null")
        } else if ( playerData == 'E' ) {
            console.log("asyncRequestPlayer result : error")
        } else if ( playerData == 'P' ) {
            console.log("asyncRequestPlayer result : pass")
            await insertSimplePlayerByLineup(player.player_fa_id);
        } else if ( playerData == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncConvertNationalPlayers 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            console.log('2-1')
            await asyncInsertPlayer(playerData);
            console.log('2-2')
        }
    }

    console.log("-----asyncConvertPlayersToDate 완료-----")
}

/**
 * ────────────────────────────────────────────────────────
 * 1. 플레이어 아이디로 데이터를 컨버트한다.
 * ────────────────────────────────────────────────────────
 * @param {Football API 플레이어 아이디} player_fa_id 
 */
exports.converterPlayerToId = async function(player_fa_id) {
    
    //------------------------------------------------------
    // Step1. Football API로 선수정보를 요청한다.
    //------------------------------------------------------
    var playerData = await asyncRequestPlayer(player_fa_id)

    //------------------------------------------------------
    // Step2. 선수정보 입력작업
    //------------------------------------------------------
    if ( playerData == null ) {
        console.log("asyncRequestPlayer result : data null")
        return;
    } else if ( playerData == 'E' ) {
        console.log("asyncRequestPlayer result : error")
        return;
    } else if ( playerData == 'P' ) {
        console.log("asyncRequestPlayer result : pass")
        return;
    } else if ( playerData == 'L') {
        //
        // 요청제한
        //
        console.log("-----converterPlayerToId 요청 제한으로 종료-----")
        return;
    } else {
        //
        // Football API에서 받은 정보로 선수정보를 입력한다.
        //
        await asyncInsertPlayer(playerData);
    }

    console.log("-----converterPlayerToId 완료-----")    
}


/**
 * ────────────────────────────────────────────────────────
 * 2. 매치일자로 선수 정보를 컨버전한다.
 * ────────────────────────────────────────────────────────
 * @param {매치 시작일자} fromdate 
 * @param {매치 종료일자} todate 
 */
exports.convertPlayersToDate = async function(fromdate, todate) {

    //------------------------------------------------------
    // Step1. 선수 리스트를 쿼리한다.
    //------------------------------------------------------
    var playerList = await asyncGetPlayersToDate(fromdate, todate);
    
    if ( playerList == null || playerList.length == 0 ) {
        console.log(team_id + ' 선수 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < playerList.length; i++) {

        var player = playerList[i];

        //------------------------------------------------------
        // Step2. Football API로 선수정보를 요청한다.
        //------------------------------------------------------
        var playerData = await asyncRequestPlayer(player.player_fa_id)

        //------------------------------------------------------
        // Step3. 선수정보 입력작업
        //------------------------------------------------------
        if ( playerData == null ) {
            console.log("asyncRequestPlayer result : data null")
        } else if ( playerData == 'E' ) {
            console.log("asyncRequestPlayer result : error")
        } else if ( playerData == 'P' ) {
            console.log("asyncRequestPlayer result : pass")
        } else if ( playerData == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncConvertNationalPlayers 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            await asyncInsertPlayer(playerData);
        }
    }

    console.log("-----asyncConvertPlayersToDate 완료-----")
}


function getQuery(query) {

   
    let parameter = [  ];

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log("getQuery() 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

let asyncGetPlayersToDate = (fromdate, todate) => {

    let query = 
    `
    select distinct b.player_fa_id
    from   matches a
         , lineup b
    where  date(a.match_date) between ? and ?
    and    a.match_fa_id = b.match_fa_id
    and    not exists ( select 'x'
                        from   players x
                        where  b.player_fa_id = x.player_fa_id
                        #and   x.retry_count in ( 0, 1 )
                        and    x.updated between date_sub(date(now()), interval 6 day) 
                                             and date_add(date(now()), interval 1 day)
                      )
    order by b.player_fa_id
    `

    let parameter = [ fromdate, todate ];

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
};


/**
 * ────────────────────────────────────────────────────────
 * 3. 라인업에 있는 선수정보로 컨버트한다.
 * ────────────────────────────────────────────────────────
 */
exports.convertPlayersLineupNotExists = async function() {

    //------------------------------------------------------
    // Step1. 선수 리스트를 쿼리한다.
    //------------------------------------------------------
    var playerList = await asyncGetPlayersLineupNotExists();
    
    if ( playerList == null || playerList.length == 0 ) {
        console.log('[convertPlayersLineupNotExists] 대상 선수 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < playerList.length; i++) {

        var player = playerList[i];

        //------------------------------------------------------
        // Step2. Football API로 선수정보를 요청한다.
        //------------------------------------------------------
        var playerData = await asyncRequestPlayer(player.player_fa_id)

        //------------------------------------------------------
        // Step3. 선수정보 입력작업
        //------------------------------------------------------
        if ( playerData == null || playerData == 'E' || playerData == 'P' ) {
                
            // 선수정보가 없으면 라인업 정보로 선수정보를 입력한다.
            var simplePlayer = [];
            simplePlayer.player_fa_id = player.player_fa_id;
            simplePlayer.player_name = player.player_name;
            simplePlayer.team_fa_id = player.team_fa_id;
            simplePlayer.team_name = player.team_name;
            simplePlayer.team_national_fa_id = player.team_national_fa_id;
            simplePlayer.team_national_name = player.team_national_name;

            await asyncInsertSimplePlayer3(simplePlayer);

        } else if ( playerData == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncConvertNationalPlayers 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            await asyncInsertPlayer(playerData);
        }
    }

    // players 마스터 테이블에 있는 정보로 lineup.player_id 정보 업데이트
    await asyncUpdateLineupPlayerId(player.match_fa_id);

    console.log("-----convertPlayersLineupNotExists 완료-----")
}

async function asyncInsertSimplePlayer3(player) {

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
                console.log(' - (7) match_fa_id : ' + match_id + ' Update UpdateLineupPlayerId error : ' + err);
                reject(err);
            } else {
                console.log(' - (7) match_fa_id : ' + match_id + ' Update UpdateLineupPlayerId complete : ' + result.affectedRows);
                resolve(result);
            }
        });
    });
}


let asyncGetPlayersLineupNotExists = () => {

    let query = 
    `
    select distinct a.player_fa_id
    from   lineup a
        ,  matches b
        ,  competitions c
    where  a.match_fa_id = b.match_fa_id
    and    b.comp_fa_id = c.comp_fa_id
    and    ifnull(c.is_active, 0) = 1
    #and    date(b.match_date) = date(now())
    and   not exists ( select 'x'
                       from   players x
                       where  a.player_fa_id = x.player_fa_id
                       #and    x.retry_count in ( 0, 1 )
                       #and    x.updated between date_sub(date(now()), interval 2 day) 
                       #                     and date_add(date(now()), interval 1 day)
                     )
    #--------------------------------------------
    #and    b.match_id = 252056
    #--------------------------------------------
    order by a.player_fa_id     
    `
    
    
    // 월드컵 임시 쿼리
    // `
    // select l.player_fa_id
    //     ,  l.player_name
    //     ,  case when t.is_national != 1 then t.team_fa_id else null end team_fa_id
    //     ,  case when t.is_national != 1 then t.team_name  else null end team_name
    //     ,  case when t.is_national  = 1 then t.team_fa_id else null end team_national_fa_id
    //     ,  case when t.is_national  = 1 then t.team_name  else null end team_national_name
    // from   competitions a
    //     ,  matches m
    //     ,  lineup l
    //        left join teams t on l.team_id = t.team_id
    // where  ifnull(a.is_active, 0) = 1
    // and    a.comp_fa_id = m.comp_fa_id
    // and    m.match_fa_id = l.match_fa_id
    // and    not exists( select 'x' from players p where l.player_id = p.player_id and ifnull(p.is_user_add, 0) < 1)
    // group by m.match_fa_id, l.player_fa_id
    // ;    
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
};

/*------------------------------------------------------------------------------*/

/**
 * ────────────────────────────────────────────────────────
 * 4. 팀 아이디로 스쿼드 데이터를 조회한 후 선수정보를 컨버트한다.
 * ────────────────────────────────────────────────────────
 * @param {팀 아이디} team_id 
 */
exports.convertTeamSquadToId = async function asyncconvertTeamSquadToId(team_id) {
    
    //------------------------------------------------------
    // Step1. 스쿼드 정보를 조회한다.
    //------------------------------------------------------
    var playerList = await asyncGetTeamSquadPlayers(team_id);  

    if ( playerList == null || playerList.length == 0 ) {
        console.log(team_id + ' 팀 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < playerList.length; i++) {
        
        var player_fa_id = playerList[i].player_fa_id

        //------------------------------------------------------
        // Step2. Football API로 선수정보를 요청한다.
        //------------------------------------------------------
        var playerData = await asyncRequestPlayer(player_fa_id)

        //------------------------------------------------------
        // Step3. 선수정보 입력작업
        //------------------------------------------------------
        if ( playerData == null ) {
            console.log("asyncRequestPlayer result : data null")
        } else if ( playerData == 'E' ) {
            console.log("asyncRequestPlayer result : error")
        } else if ( playerData == 'P' ) {
            console.log("asyncRequestPlayer result : pass")
            //
            // 선수정보가 없으면 스쿼드 정보로 선수정보를 입력한다.
            //
            var simplePlayer = [];
            simplePlayer.id = playerList[i].player_fa_id;
            simplePlayer.name = playerList[i].player_name;
            simplePlayer.nationality = playerList[i].nationality;
            var result = await asyncInsertSimplePlayer(simplePlayer)
        } else if ( playerData == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncconvertTeamSquadToId 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            await asyncInsertPlayer(playerData);
        }
    }

    console.log("-----asyncconvertTeamSquadToId 완료-----")
}

/**
 * 팀 테이블에서 squad 데이터를 선수 리스트로 변경하여 반환한다.
 *  - players 테이블에 데이터가 존재하면 리스트에서 제외한다.
 *  - updated 날짜가 5일 이내라면 리스트에서 제외한다.
 * @param {팀 아이디} team_id 
 */
let asyncGetTeamSquadPlayers = (team_id) => {

    let query = 
    `
    select a.player_fa_id, player_name, team_name, nationality
    from
    (    
        select idx
                , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].id'))) as player_fa_id
                , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].name'))) as player_name
                , case when t.is_national != 1 then t.team_name else '' end as team_name
                , case when t.is_national  = 1 then t.team_name else '' end as nationality
            from teams t
            join 
            (
                select  0 as idx union select  1 as idx union select  2 as idx union select  3 as idx union select  4 as idx union select  5 as idx union
                select  6 as idx union select  7 as idx union select  8 as idx union select  9 as idx union select 10 as idx union select 11 as idx union 
                select 12 as idx union select 13 as idx union select 14 as idx union select 15 as idx union select 16 as idx union select 17 as idx union
                select 18 as idx union select 19 as idx union select 20 as idx union select 21 as idx union select 22 as idx union select 23 as idx union
                select 24 as idx union select 25 as idx union select 26 as idx union select 27 as idx union select 28 as idx union select 29 as idx union 
                select 30 as idx union select 31 as idx union select 32 as idx union select 33 as idx union select 34 as idx union select 35 as idx union
                select 36 as idx union select 37 as idx union select 38 as idx union select 39 as idx union select 40 as idx union select 41 as idx union
                select 42 as idx union select 43 as idx union select 44 as idx union select 44 as idx union select 45 as idx union select 46 as idx 
            ) as indexes
            where t.team_id = ?
    ) a
    where player_fa_id is not null     
    and   not exists ( select 'x'
                       from   players x
                       where  a.player_fa_id = x.player_fa_id
                       #and    x.retry_count in (0, 1)
                       and    x.updated between date_sub(date(now()), interval 6 day) 
                                            and date_add(date(now()), interval 1 day)
                     )    
    `

    return new Promise((resolve, reject) => {
        db.excuteSql(query, [team_id], function (err, result){
        
            if (err) {
                console.log("asyncGetTeamSquadPlayers 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
};

/**
 * ────────────────────────────────────────────────────────
 * 5. 국대선수들 컨버팅을 한다.
 *  - Teams.squad 데이터를 조회
 *  - players 테이블에 선수정보가 있는지 확인
 *  - 없으면 선수정보 요청 -> 데이터입력
 *  - 만약 Football API에 선수정보가 없으면 player_fa_id, 이름, 국가 정보만 입력한다.
 * ────────────────────────────────────────────────────────
 */
exports.convertNationalPlayers = async function () {

    //------------------------------------------------------
    // Step1. 스쿼드 정보를 조회한다.
    //------------------------------------------------------
    var playerList = await asyncGetNationalPlayers();
    
    if ( playerList == null || playerList.length == 0 ) {
        console.log(team_id + ' 국가 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < playerList.length; i++) {

        var player = playerList[i];

        //------------------------------------------------------
        // Step2. Football API로 선수정보를 요청한다.
        //------------------------------------------------------
        var playerData = await asyncRequestPlayer(player.player_fa_id)

        //------------------------------------------------------
        // Step3. 선수정보 입력작업
        //------------------------------------------------------
        if ( playerData == null ) {
            console.log("asyncRequestPlayer result : data null")
        } else if ( playerData == 'E' ) {
            console.log("asyncRequestPlayer result : error")
        } else if ( playerData == 'P' ) {
            console.log("asyncRequestPlayer result : pass")
            //
            // 선수정보가 없으면 스쿼드 정보로 선수정보를 입력한다.
            //
            var simplePlayer = [];
            simplePlayer.id =player.player_fa_id;
            simplePlayer.name = player.player_name;
            simplePlayer.nationality = player.nationality;
            var result = await asyncInsertSimplePlayer(simplePlayer)
        } else if ( playerData == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncConvertNationalPlayers 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            await asyncInsertPlayer(playerData);
        }
    }

    console.log("-----asyncConvertNationalPlayers 완료-----")
}

/**
 * 
 * 팀(국가) 테이블에서 squad 데이터를 선수 리스트로 변경하여 반환한다.
 *  - players 테이블에 데이터가 존재하면 리스트에서 제외한다.
 *  - updated 날짜가 5일 이내라면 리스트에서 제외한다.
 * 
 */
let asyncGetNationalPlayers = () => {

    let query = 
    `
    select a.player_fa_id, a.player_name, a.nationality
    from
    (    
        select idx
                , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].id'))) as player_fa_id
                , JSON_UNQUOTE(json_extract(t.squad, concat('$[', idx, '].name'))) as player_name
                , t.team_name nationality
            from teams t
            join 
            (
                select  0 as idx union select  1 as idx union select  2 as idx union select  3 as idx union select  4 as idx union select  5 as idx union
                select  6 as idx union select  7 as idx union select  8 as idx union select  9 as idx union select 10 as idx union select 11 as idx union 
                select 12 as idx union select 13 as idx union select 14 as idx union select 15 as idx union select 16 as idx union select 17 as idx union
                select 18 as idx union select 19 as idx union select 20 as idx union select 21 as idx union select 22 as idx union select 23 as idx union
                select 24 as idx union select 25 as idx union select 26 as idx union select 27 as idx union select 28 as idx union select 29 as idx union 
                select 30 as idx union select 31 as idx union select 32 as idx union select 33 as idx union select 34 as idx union select 35 as idx union
                select 36 as idx union select 37 as idx union select 38 as idx union select 39 as idx union select 40 as idx union select 41 as idx union
                select 42 as idx union select 43 as idx union select 44 as idx union select 44 as idx union select 45 as idx union select 46 as idx 
            ) as indexes
            #------------------------------
            # 국가정보만 조회
            #-----------------------------
            where t.is_national = 1
            #and   t.team_id = 2195
    ) a
    where a.player_fa_id is not null     
    and   not exists ( select 'x'
                       from   players x
                       where  a.player_fa_id = x.player_fa_id
                       #and    x.retry_count in ( 0, 1 )
                       and    x.updated between date_sub(date(now()), interval 6 day) 
                                                   and date_add(date(now()), interval 1 day)
                     )    
	 order by a.nationality   
    `

    return new Promise((resolve, reject) => {
        db.excuteSql(query, null, function (err, result){
        
            if (err) {
                console.log("asyncGetNationalPlayers 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
};


/*-------------------------------------------------------------*/


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

/**
 * Football API에서 가져온 데이터로 디비에 입력
 * @param {Football API 선수아이디} player_data 
 */
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
                            0, //retry_count
                            0 // is_user_add
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
            national_teamid    = national_teamid, 
            nationality        = VALUES(nationality),
            birthdate          = VALUES(birthdate),
            birthcountry       = VALUES(birthcountry),
            birthplace         = VALUES(birthplace),
            height             = VALUES(height),
            weight             = VALUES(weight),
            age                = VALUES(age),
            position           = VALUES(position),
            player_statistics  = VALUES(player_statistics),
            hits               = hits,
            tag                = tag,
            updated            = now(),
            retry_count        = 0,
            is_user_add        = VALUES(is_user_add),
            overview           = overview,
            retry_count        = retry_count,
            mentioned          = mentioned,
            retired            = retired,
            coach_id           = coach_id
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


/**
 * Football API에 선수정보가 없으면 teams.squad 정보로 데이터를 입력한다.
 * @param {Football API 선수아이디} player_data 
 */
function asyncInsertSimplePlayer(player) {

    return new Promise((resolve, reject) => {

        //let player = JSON.parse(player_data);
        //player.nationality = nationality
        let playerParamter = []

        playerParamter.push([player.id,
                            player.name.replace("'", "\\'"),
                            player.nationality,
                            1
                        ]);

        let query =
        `
        INSERT INTO players 
        (   player_fa_id,
            player_name,
            nationality,
            is_user_add
        ) 
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
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
                console.log("asyncInsertSimplePlayer " + player.id + " 데이터 입력 완료")
                resolve(result)
            }
        })

    });
}

/**
 * Football API에 선수정보가 없을때
 * 라인업 정보로 Players 테이블에 데이터를 입력한다.
 * 단, 라인업에도 선수정보가 없으면 players.updated 값이 업데이트가 안된다.
 * @param {*} player_fa_id 
 */
function insertSimplePlayerByLineup(player_fa_id) {

    return new Promise((resolve, reject) => {

        //let player = JSON.parse(player_data);
        //player.nationality = nationality
        let playerParamter = [player_fa_id]

        let query =
        `
        insert into players ( player_id, player_fa_id, player_name, teamid, team, is_user_add, updated)
        select player_id
            ,  player_fa_id
            ,  player_name
            ,  team_fa_id teamid
            ,  ( select t.team_name
                 from   teams t
                 where  l.team_fa_id = t.team_fa_id) team
            ,  1    is_user_add
            ,  now() updated
        from   lineup l
        where  player_fa_id = ?
        and    match_fa_id  = ( select max(x.match_fa_id)
                                from   lineup x
                                where  l.player_fa_id = x.player_fa_id )
        ON DUPLICATE KEY UPDATE 
        teamid      = values(teamid),
        team        = values(team),
        is_user_add = values(is_user_add),
        updated     = now()
        ;
        `

        db.excuteSql(query, [playerParamter], (err, result)=>{
            if (err) {
                reject(err)
            } else {
                //callback(null, result);
                console.log("insertSimplePlayerByLineup " + player_fa_id + " 데이터 입력 완료")
                resolve(result)
            }
        })

    });
}


/**
 * 선수정보가 없을때 라인업 정보로 선수등록을 한다.
 * @param {*} player_fa_id 
 */
function asyncInsertSimplePlayer2(player_fa_id) {

    return new Promise((resolve, reject) => {

        let query =
        `
        insert into players(player_fa_id, player_name, is_user_add)
        select a.player_fa_id
            ,  a.player_name
            ,  1
        from   lineup a
        where  a.player_fa_id = ?
        and    a.match_fa_id = ( select max(x.match_fa_id)
                                 from   lineup x
                                 where  a.player_fa_id = x.player_fa_id
                               )
        ;
        `

        db.excuteSql(query, [player_fa_id], (err, result)=>{
            if (err) {
                reject(err)
            } else {
                //callback(null, result);
                console.log("asyncInsertSimplePlayer2 " + player_fa_id + " 데이터 입력 완료")
                resolve(result)
            }
        })

    });
}



/*-------------------------------------------------------------*/



/**
 * 플레이어 리스트를 대상으로 선수정보를 요청하고 데이터베이스에 데이터를 입력한다.
 * @param {*} player_list 
 * @param {*} callback 
 */
function loopPlayers(player_list, callback) {
    
    let index = 0;

    async.whilst(
        function(callback) {
            return index < player_list.length;
        },
        function(callback) {

            let player_id = player_list[index].player_fa_id;
            console.log(' - [loopPlayer] ' + (index+1) + ' / ' + player_list.length + ', player id : ' + player_id);
            index++;

            request_insert_player(player_id, function(err, result){
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


/**
 * 선수정보를 요청하고 데이터베이스에 데이터를 입력한다.
 * 1. 선수정보 요청
 * 2. 디비에 데이터 입력
 * @param {*} player_id 
 * @param {*} callback 
 */
function request_insert_player(player_id, callback) {
    
    var tasks = [
        function (callback01) {
            //---------------------------------
            // 1. Football API 선수 데이터 요청
            //---------------------------------
            requestPlayer(player_id, function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                }
                return callback01(null, result);
            })
        },
        function (player_data, callback02) {
            
            if ( player_data == 'next' ) {
                console.log(' - [request_insert_players] player data empty'); 
                return callback02(null, player_data);
            }

            //---------------------------------
            // 2. 선수 데이터 입력
            //---------------------------------
            insertPlayer(player_data, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                }
                return callback02(null, result);
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

function getPlayers(fromdate, todate, callback) {
    
    console.log(' - [getConversionPlayerList]');

    let query = 
    `
    select distinct b.player_fa_id
    from   matches a
         , lineup b
    where  date(a.match_date) between ? and ?
    and    a.match_fa_id = b.match_fa_id
    and    not exists ( select 'x'
                        from  players x
                        where x.player_fa_id = b.player_fa_id)
    and    not exists ( select 'x'
                        from   convert_log x
                        where  x.code = 'CP'
                        and    x.errcode = '404'
                        and    b.player_fa_id = x.subcode)                    
    order by b.player_fa_id  
    `

    let parameter = [ fromdate, todate ];
    
    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}


/**
 * 시작 / 종료 날짜 기준으로 누락된 플레이어 리스트를 조회한다.
 * @param {*} fromdate 
 * @param {*} todate 
 * @param {*} callback 
 */
function getPlayersToDate(fromdate, todate, callback) {
    
    console.log(' - [getConversionPlayerList]');

    let query = 
    `
    select distinct b.player_fa_id
    from   matches a
         , lineup b
    where  date(a.match_date) between ? and ?
    and    a.match_fa_id = b.match_fa_id
    and    not exists ( select 'x'
                        from  players x
                        where x.player_fa_id = b.player_fa_id)
    and    not exists ( select 'x'
                        from   convert_log x
                        where  x.code = 'CP'
                        and    x.errcode = '404'
                        and    b.player_fa_id = x.subcode)                        
    order by b.player_fa_id  
    `

    let parameter = [ fromdate, todate ];
    
    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}

function getPlayersNotExists(callback) {
    
    console.log(' - [getPlayersNotExists]');

    let query = 
    // `
    // select distinct a.player_fa_id
    // from   lineup a
    // where  not exists ( select 'x' from players b where a.player_fa_id = b.player_fa_id)
    // and    not exists ( select 'x'
    //                     from   convert_log x
    //                     where  x.code = 'CP'
    //                     and    x.errcode = '404'
    //                     and    a.player_fa_id = x.subcode)                        
    // order by a.player_fa_id  
    // `

    `
    select distinct a.player_fa_id
    from   lineup a
    where  not exists ( select 'x' from players b where a.player_fa_id = b.player_fa_id)
    order by a.player_fa_id  
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
 * 팀 라인업에서 플레이어 마스터 테이블에 누락된 플레이어 리스트를 조회한다.
 * @param {*} team_fa_id 
 * @param {*} callback 
 */
function getPlayersToTeam(team_fa_id, callback) {
    
    console.log(' - [getConversionPlayerListToTeam]');

    let query = 
    `
    select distinct a.player_fa_id  
    from   lineup a
    where  a.team_fa_id = ?   
    and    not exists ( select 'x'
                        from   players x
                        where  x.player_fa_id = a.player_fa_id)
    and    not exists ( select 'x'
                        from   convert_log x
                        where  x.code = 'CP'
                        and    x.errcode = '404'
                        and    b.player_fa_id = x.subcode)                        
    order by a.player_fa_id    
    `

    let parameter = [ team_fa_id ];
    
    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result);
        }
    });
}


/**
 * football api 로 플레이어 데이터를 요청한다.
 * @param {*} player_id 
 * @param {*} callback 
 */
function requestPlayer(player_id, callback) {
    
    console.log(' - [requestPlayer]');

    // 헤더 부분
    var headers = {
        'User-Agent':       'Super Agent/0.0.1',
        'Content-Type':     'application/json'
        //'Content-Type':     'application/x-www-form-urlencoded'
    }

    var options = {
        url : 'http://api.football-api.com/2.0/player/' + player_id,
        headers: headers,
        method : 'GET',
        qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
    }

    request.get(options, function (error, response, body) {

        if ( error )  {
            return callback(error, null);
        } else if ( !error && response.statusCode != 200 ) {
            
            var log = {};
            log.code = 'CP';
            log.subcode = player_id;
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




/**
 * 하이파이브 풋볼 디비에 선수 정보를 입력한다.
 * @param {*} player_data 
 * @param {*} callback 
 */
function insertPlayer(player_data, callback) {
    
    if ( player_data == 'next' ) {
        console.log(' - [insertPlayer] data empty');    
        return callback(null, 'next');
    }

    console.log(' - [insertPlayer]');

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

    //console.log('birthdate : ' + birthdate)
    //console.log('player.firstname : ' + player.firstname.replace("'", "\\'") + " , length " + player.firstname.replace("'", "\\'").length)
    //console.log('birthplace : ' + player.birthplace + ", length : " + player.birthplace.length)

    let common_name;
    if ( player.common_name) {
        common_name = player.common_name.replace("'", "\\'")
    } else {
        common_name = undefined;
    }

    playerParamter.push([player.id,
                        player.name.replace("'", "\\'"),
                        //player.common_name != null ? player.common_name.replace("'", "\\'") : undefined, "" +
                        common_name,
                        player.firstname.replace("'", "\\'"),
                        player.lastname.replace("'", "\\'").substring(0, 30),
                        player.team,
                        player.teamid,
                        player.nationality,
                        birthdate ? birthdate : undefined,
                        player.age ? player.age : undefined,
                        player.birthcountry,
                        player.birthplace,
                        player.position,
                        player.height,
                        player.weight,
                        player.player_statistics ? JSON.stringify(player.player_statistics) : undefined
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
        player_statistics) 
    VALUES ? 
    ON DUPLICATE KEY UPDATE 
        team              = VALUES(team),
        teamid            = VALUES(teamid),
        nationality       = VALUES(nationality),
        age               = VALUES(age),
        position          = VALUES(position),
        player_statistics = VALUES(player_statistics),
        updated           = now()
        ;
    `

    db.excuteSql(query, [playerParamter], (err, result)=>{
        if (err) {
            console.log(' - [insertPlayer] error : ' + err);

            var log = {};
            log.code = 'CP';
            log.subcode = player.id;
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



/**
 * 하이파이브 풋볼 디비에 선수 정보를 입력한다.
 * @param {*} player_data 
 * @param {*} callback 
 */
function insertPlayer2(player_data, callback) {
    
    if ( player_data == 'next' ) {
        console.log(' - [insertPlayer] data empty');    
        return callback(null, 'next');
    }

    console.log(' - [insertPlayer]');

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

    //console.log('birthdate : ' + birthdate)
    //console.log('player.firstname : ' + player.firstname.replace("'", "\\'") + " , length " + player.firstname.replace("'", "\\'").length)
    //console.log('birthplace : ' + player.birthplace + ", length : " + player.birthplace.length)

    let common_name;
    if ( player.common_name) {
        common_name = player.common_name.replace("'", "\\'")
    } else {
        common_name = undefined;
    }

    playerParamter.push([player.id,
                        player.name.replace("'", "\\'"),
                        //player.common_name != null ? player.common_name.replace("'", "\\'") : undefined, "" +
                        common_name,
                        player.firstname.replace("'", "\\'"),
                        player.lastname.replace("'", "\\'").substring(0, 30),
                        player.team,
                        player.teamid,
                        player.nationality,
                        birthdate ? birthdate : undefined,
                        player.age ? player.age : undefined,
                        player.birthcountry,
                        player.birthplace,
                        player.position,
                        player.height,
                        player.weight,
                        player.player_statistics ? JSON.stringify(player.player_statistics) : undefined
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
        player_statistics) 
    VALUES ? 
    ON DUPLICATE KEY UPDATE 
        team              = VALUES(team),
        teamid            = VALUES(teamid),
        nationality       = VALUES(nationality),
        age               = VALUES(age),
        position          = VALUES(position),
        player_statistics = VALUES(player_statistics),
        updated           = now()
        ;
    `

    db.excuteSql(query, [playerParamter], (err, result)=>{
        if (err) {
            console.log(' - [insertPlayer] error : ' + err);

            var log = {};
            log.code = 'CP';
            log.subcode = player.id;
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


/*-------------------------------------------------------------*/


function stringToDate(_date,_format,_delimiter)
{
    var formatLowerCase=_format.toLowerCase();
    var formatItems=formatLowerCase.split(_delimiter);
    var dateItems=_date.split(_delimiter);
    var monthIndex=formatItems.indexOf("mm");
    var dayIndex=formatItems.indexOf("dd");
    var yearIndex=formatItems.indexOf("yyyy");
    var month=parseInt(dateItems[monthIndex]);
    month-=1;
    var formatedDate = new Date(dateItems[yearIndex],month,dateItems[dayIndex]);
    return formatedDate;
}    

String.prototype.format = function()
{
   var content = this;
   for (var i=0; i < arguments.length; i++)
   {
        var replacement = '{' + i + '}';
        content = content.replace(replacement, arguments[i]);  
   }
   return content;
};