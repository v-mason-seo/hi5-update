const db = require('./db');
const request = require('request');
const async = require('async');

const moment = require('moment');
const moment2 = require('moment-timezone');
var dateFormat = require('dateformat');


/**
 * ────────────────────────────────────────────────────────
 * 1. competitions 테이블에 등록된 모든 리그의 순위표를 컨버팅한다.
 * ────────────────────────────────────────────────────────
 */
exports.converterStandings = async () => {

    //------------------------------------------------------
    // Step1. 컴피티션 리스트를 쿼리한다.
    //------------------------------------------------------
    var competitionList = await asyncQueryActiveCompetitionList();

    if ( competitionList == null || competitionList.length == 0 ) {
        console.log(team_id + '컴피티션 정보가 없습니다.')
        return;
    }

    for ( i = 0; i < competitionList.length; i++) {

        var competition = competitionList[i];

        //------------------------------------------------------
        // Step2. Football API로 Standings 정보를 요청한다.
        //------------------------------------------------------
        var standing = await asyncRequestStandings(competition.comp_fa_id);


        //------------------------------------------------------
        // Step3. Standings 정보 입력작업
        //------------------------------------------------------
        if ( standing == null ) {
            console.log("asyncRequestStandings result : data null")
        } else if ( standing == 'E' ) {
            console.log("asyncRequestStandings result : error")
        } else if ( standing == 'P' ) {
            console.log("asyncRequestStandings result : pass")
        } else if ( standing == 'L') {
            //
            // 요청제한
            //
            console.log("-----asyncConverterStandings 요청 제한으로 종료-----")
            return;
        } else {
            //
            // Football API에서 받은 정보로 선수정보를 입력한다.
            //
            await asyncInsertStandings(standing);
        }
    }

    console.log("-----asyncConverterStandings 완료-----")
};


/**
 * 활성화된 컴피티션 리스트를 쿼리한다.
 */
let asyncQueryActiveCompetitionList = () => {

    let query = 
    `
    select a.comp_id, a.comp_fa_id
    from   arena.competitions a
    where  a.is_active = 1
    order by a.comp_id       
    `

    let parameter = [];

    return new Promise((resolve, reject) => {
        db.excuteSql(query, parameter, function (err, result){
        
            if (err) {
                console.log("asyncGetActiveCompetitionLIst 실패 - " + err)
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
};



/**
 * ────────────────────────────────────────────────────────
 * 2. comp_fa_id 로 하나의 리그 순위표를 컨버팅한다.
 * ────────────────────────────────────────────────────────
 * @param {*} comp_fa_id 
 */
exports.converterStandingToId = async (comp_fa_id) => {

    //------------------------------------------------------
    // Step1. Football API로 Standings 정보를 요청한다.
    //------------------------------------------------------
    var standing = await asyncRequestStandings(comp_fa_id);

    
    //------------------------------------------------------
    // Step2. Standings 정보 입력작업
    //------------------------------------------------------
    if ( standing == null ) {
        console.log("asyncRequestStandings result : data null")
    } else if ( standing == 'E' ) {
        console.log("asyncRequestStandings result : error")
    } else if ( standing == 'P' ) {
        console.log("asyncRequestStandings result : pass")
    } else if ( standing == 'L') {
        //
        // 요청제한
        //
        console.log("-----asyncConverterStandingsToId 요청 제한으로 종료-----")
        return;
    } else {
        //
        // Football API에서 받은 정보로 선수정보를 입력한다.
        //
        await asyncInsertStandings(standing);
    }

    console.log("-----asyncConverterStandingsToId 완료-----")
};


/*--------------------------------------------------------*/


/**
 * 
 * Football API 에 standings 데이터를 요청한다.
 * 
 * @param {컴피티션 아이디} comp_id 
 */
let asyncRequestStandings = (comp_id) => {
    
    return new Promise((resolve, reject) => {

        // 헤더 부분
        var headers = {
            'User-Agent':       'Super Agent/0.0.1',
            'Content-Type':     'application/json'
            //'Content-Type':     'application/x-www-form-urlencoded'
        }

        var options = {
            url : 'http://api.football-api.com/2.0/standings/' + comp_id,
            headers: headers,
            method : 'GET',
            qs: {'Authorization': '565ec012251f932ea40000018ded3bec30d640926ceb57121f7204fa'}
        }

        request.get(options, function (error, response, body) {

            if ( error )  {
                reject('E'); // error
            } else if ( !error && response.statusCode != 200 ) {

                if ( response.statusCode == 429 ) {
                    console.log("asyncRequestStandings " + comp_id + response.body.toString())
                    // limit - 요청제한 초과
                    resolve('L'); 
                } else {
                    console.log("asyncRequestStandings " + comp_id + response.body.toString())
                    // pass - 데이터 없음
                    resolve('P'); 
                }
                
            } else {
                console.log("asyncRequestStandings " + comp_id + " standings 요청 완료")
                resolve(body);
            }
        })
    });
}


/**
 * 
 * 하이파이브 디비로 standings 데이터를 입력한다.
 * 
 * @param {*} body 
 * @param {*} callback 
 */
let asyncInsertStandings = (body) => {

    return new Promise((resolve, reject) => {

        let standings = JSON.parse(body);
        let standingParameter = [];

        standings.map ( function(standing, i) {
            standingParameter.push([standings[i].comp_id, "" + 
                                    standings[i].season, "" + 
                                    standings[i].round, "" + 
                                    standings[i].stage_id, "" + 
                                    standings[i].comp_group ? standings[i].comp_group : undefined, "" + 
                                    standings[i].country, "" + 
                                    standings[i].team_id, "" + 
                                    standings[i].team_name, "" + 
                                    standings[i].status, "" + 
                                    standings[i].recent_form, "" + 
                                    standings[i].position, "" +
                                    //--------------------------
                                    standings[i].overall_gp ? standings[i].overall_gp : 0, "" + 
                                    standings[i].overall_w ? standings[i].overall_w : 0, "" +
                                    standings[i].overall_d ? standings[i].overall_d : 0, "" +
                                    standings[i].overall_l ? standings[i].overall_l : 0, "" +
                                    standings[i].overall_gs ? standings[i].overall_gs : 0, "" +
                                    standings[i].overall_ga ? standings[i].overall_ga : 0, "" +
                                    //--------------------------
                                    standings[i].home_gp ? standings[i].home_gp : 0, "" +
                                    standings[i].home_w ? standings[i].home_w : 0, "" +
                                    standings[i].home_d ? standings[i].home_d : 0, "" +
                                    standings[i].home_l ? standings[i].home_l : 0, "" +
                                    standings[i].home_gs ? standings[i].home_gs : 0, "" +
                                    standings[i].home_ga ? standings[i].home_ga : 0, "" +
                                    //--------------------------
                                    standings[i].away_gp ? standings[i].away_gp : 0, "" +
                                    standings[i].away_w ? standings[i].away_w : 0, "" +
                                    standings[i].away_d ? standings[i].away_d : 0, "" +
                                    standings[i].away_l ? standings[i].away_l : 0, "" +
                                    standings[i].away_gs ? standings[i].away_gs : 0, "" +
                                    standings[i].away_ga ? standings[i].away_ga : 0, "" +
                                    //--------------------------
                                    standings[i].gd, "" +
                                    standings[i].points, "" +
                                    standings[i].description
                                ]);
                    })
        
                    let query =
                    `
                    INSERT INTO arena.standings ( comp_fa_id, 
                                                        season, 
                                                        round, 
                                                        stage_id, 
                                                        comp_group, 
                                                        country,
                                                        team_fa_id, 
                                                        team_name, 
                                                        status, 
                                                        recent_form, 
                                                        position,
                                                        #-------------------------------------------------------------------
                                                        overall_gp, overall_w, overall_d, overall_l, overall_gs, overall_ga,
                                                        #-------------------------------------------------------------------
                                                        home_gp, home_w, home_d, home_l, home_gs, home_ga, 
                                                        #-------------------------------------------------------------------
                                                        away_gp, away_w, away_d, away_l, away_gs, away_ga,
                                                        #-------------------------------------------------------------------
                                                        gd,
                                                        points,
                                                        description ) 
                                                        VALUES ? 
                    ON DUPLICATE KEY UPDATE 
                    stage_id    = VALUES(stage_id),
                    comp_group  = VALUES(comp_group),
                    country     = VALUES(country),
                    status      = VALUES(status),
                    recent_form = VALUES(recent_form),
                    position    = VALUES(position),
                    #----------------------------------------
                    overall_gp  = VALUES(overall_gp),
                    overall_w   = VALUES(overall_w),
                    overall_d   = VALUES(overall_d),
                    overall_l   = VALUES(overall_l),
                    overall_gs  = VALUES(overall_gs),
                    overall_ga  = VALUES(overall_ga),
                    #----------------------------------------
                    home_gp     = VALUES(home_gp),
                    home_w      = VALUES(home_w),
                    home_d      = VALUES(home_d),
                    home_l      = VALUES(home_l),
                    home_gs     = VALUES(home_gs),
                    home_ga     = VALUES(home_ga),
                    #----------------------------------------
                    away_gp     = VALUES(away_gp),
                    away_w      = VALUES(away_w),
                    away_d      = VALUES(away_d),
                    away_l      = VALUES(away_l),
                    away_gs     = VALUES(away_gs),
                    away_ga     = VALUES(away_ga),
                    #----------------------------------------
                    gd          = VALUES(gd),
                    points      = VALUES(points),
                    description = VALUES(description),
                    updated     = now()
                    ;
                    `

        db.excuteSql(query, [standingParameter], (err, result)=>{
            if (err) {
                return reject(err)
            }

            console.log("asyncInsertStandings 데이터 입력 완료")
            return resolve(result)
        })
    })
}

/*--------------------------------------------------------*/