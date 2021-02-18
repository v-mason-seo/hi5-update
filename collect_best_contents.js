const db = require('./db');
const request = require('request');
const async = require('async');

const moment = require('moment');
const moment2 = require('moment-timezone');

/*-------------------------------------------------------------*/

exports.collectBestContents = (best_type) => {

    board_list = [200, 300, 350, 400, 420];
    

    console.log('[collectBestContents(댓글)] start');
    loopBestContents(best_type, 'C', board_list, function(err, result) {

        if ( err ) {
            console.log('[collectBestContents(댓글)] error : ' + err);
            return 0;
        } else {
            console.log('[collectBestContents(댓글)] compete');
            return 1;
        }
    });

    loopBestContents(best_type, 'L', board_list, function(err, result) {

        if ( err ) {
            console.log('[collectBestContents(하이파이브)] error : ' + err);
            return 0;
        } else {
            console.log('[collectBestContents(하이파이브)] compete');
            return 1;
        }
    });
};

exports.collectForceBestContents = (best_type, fromdate, todate) => {

    board_list = [200, 300, 350, 400, 420];
    

    console.log('[collectForceBestContents(댓글)] start');
    loopForceBestContents(best_type, 'C', board_list, fromdate, todate, function(err, result) {

        if ( err ) {
            console.log('[collectForceBestContents(댓글)] error : ' + err);
            return 0;
        } else {
            console.log('[collectForceBestContents(댓글)] compete');
            return 1;
        }
    });

    loopForceBestContents(best_type, 'L', board_list, fromdate, todate, function(err, result) {

        if ( err ) {
            console.log('[collectForceBestContents(하이파이브)] error : ' + err);
            return 0;
        } else {
            console.log('[collectForceBestContents(하이파이브)] compete');
            return 1;
        }
    });
};


/*-------------------------------------------------------------*/

/**
 * 
 * @param {*} best_type    D: 일간베스트, W: 주간베스트, M: 월간베스트
 * @param {*} best_roll    L: 하이파이브, C: 댓글
 * @param {*} board_list
 * @param {*} callback 
 */
function loopBestContents(best_type, best_roll, board_list, callback) {

    let index = 0;

    async.whilst (
        function(callback01) {
            return index < board_list.length;
        },
        function(callback02) {

            let board_id = board_list[index];
            console.log(' - [loopBestContents - ' + best_type + ', ' + best_roll+ ' ] ' + (index + 1) + ' / ' + board_list.length + ', board id : ' + board_id);
            index++;

            insert_update_bestContents(best_type, best_roll, board_id, function(err, result) {
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

function insert_update_bestContents(best_type, best_roll, board_id, callback) {

    var tasks = [
        function(callback01) {
            getCurrentDate(best_type, function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                } 

                var best_contents = {};
                best_contents.fromdate = result[0].fromdate;
                best_contents.todate = result[0].todate;
                best_contents.best_id = result[0].best_id;
                best_contents.best_div = result[0].best_div;
                best_contents.best_type = best_type;
                best_contents.best_roll = best_roll;
                best_contents.board_id = board_id;

                return callback01(null, best_contents);
            })
        },
        function(best_contents, callback02) {
            insertBestContents(best_contents, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                } else {
                    if ( result.affectedRows == 0 ) {
                        return callback02(null, null);
                    }

                    return callback02(null, best_contents);
                }
            })
        },
        function(best_contents, callback03) {

            if ( !best_contents ) {
                return callback03(null, '데이터 없음');
            }
            updateBestContents(best_contents, function(err, result) {
                if ( err ) {
                    return callback03(err, null);
                } else {
                    return callback03(null, result);
                }
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

function getCurrentDate(best_type, callback) {

    // let query;
    // let parameter;

    // if ( fromdate) {
    //     query = 
    //     `
    //     select ?                              fromdate
    //         ,  ?                              todate
    //         ,  date_format(todate, '%Y%m%d')  best_id
    //         ,  date_format(todate, '%H')      best_div
    //     `

    //     parameter = [ fromdate, todate ];
    // } else {
        
    // }

    //%U	Week (00..53), where Sunday is the first day of the week
    //%u	Week (00..53), where Monday is the first day of the week
    //%V	Week (01..53), where Sunday is the first day of the week; used with %X
    //%v	Week (01..53), where Monday is the first day of the week; used with %x

    let query = 
        `
        select date_sub(now(), interval case when ? = 'D' then 1
                                             when ? = 'W' then 7
                                             when ? = 'M' then last_day(now()) end day) fromdate
            ,  now()                         todate
            ,  date_format(now(), '%Y%m%d')  best_id
            ,  date_format(now(), '%H')      best_div
        `

    let parameter = [ best_type, best_type, best_type ];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            console.log(' - [getCurrentDate] error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - [getCurrentDate] compete');
            return callback(null, result);
        }
    });
}



function insertBestContents(best_contents, callback) {

    let query;

    if ( best_contents.best_roll == 'C') {
        query = 
        `
        insert ignore hifive.best_contents ( best_id, best_div, best_type, best_roll, board_id, content_id, latest)
        select ? best_id
            ,  ? best_div
            ,  ? best_type
            ,  ? best_roll
            ,  a.board_id
            ,  a.content_id
            ,  1 latest
        from   hifive.contents a
        where  a.deleted = 0
        and    a.reported = 0
        and    a.comments > 0
        and    a.board_id = ?
        and    a.created between ? and ?
        order by a.comments desc
        limit 10;		
        `
    } else {
        query = 
        `
        insert ignore hifive.best_contents ( best_id, best_div, best_type, best_roll, board_id, content_id, latest)
        select ? best_id
            ,  ? best_div
            ,  ? best_type
            ,  ? best_roll
            ,  a.board_id
            ,  a.content_id
            ,  1 latest
        from   hifive.contents a
        where  a.deleted = 0
        and    a.reported = 0
        and    a.likers > 0
        and    a.board_id = ?
        and    a.created between ? and ?
        order by a.likers desc
        limit 10;		
        `
    }
    

    let parameter = [ best_contents.best_id, 
                    best_contents.best_div, 
                    best_contents.best_type, 
                    best_contents.best_roll, 
                    best_contents.board_id, 
                    best_contents.fromdate, 
                    best_contents.todate ];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            console.log(' - [insertBestContents] error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - [insertBestContents] complete');
            return callback(null, result);
        }
    });
}

function updateBestContents(best_contents, callback) {

    let query = 
    `
    update hifive.best_contents
    set latest = 0
    where best_id = ?
    and best_div != ?
    and best_type = ?
    and best_roll = ?
    and board_id = ?
    and latest != 0
    ;
    `

    let parameter = [ best_contents.best_id,
                    best_contents.best_div, 
                    best_contents.best_type, 
                    best_contents.best_roll, 
                    best_contents.board_id ];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            console.log(' - [updateBestContents] error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - [updateBestContents] complete');
            return callback(null, result);
        }
    });
}



/*-------------------------------------------------------------------------------------------------*/


function loopForceBestContents(best_type, best_roll, board_list, fromdate, todate, callback) {

    let index = 0;

    async.whilst (
        function(callback01) {
            return index < board_list.length;
        },
        function(callback02) {

            let board_id = board_list[index];
            console.log(' - [loopBestContents - ' + best_type + ', ' + best_roll+ ' ] ' + (index + 1) + ' / ' + board_list.length + ', board id : ' + board_id);
            index++;

            insert_update_force_bestContents(best_type, best_roll, board_id, fromdate, todate, function(err, result) {
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

function insert_update_force_bestContents(best_type, best_roll, board_id, fromdate, todate, callback) {

    var tasks = [
        function(callback01) {
            getCurrentForceDate(best_type, fromdate, todate, function(err, result) {
                if ( err ) {
                    return callback01(err, null);
                } 

                var best_contents = {};
                best_contents.fromdate = result[0].fromdate;
                best_contents.todate = result[0].todate;
                best_contents.best_id = result[0].best_id;
                best_contents.best_div = result[0].best_div;
                best_contents.best_type = best_type;
                best_contents.best_roll = best_roll;
                best_contents.board_id = board_id;

                return callback01(null, best_contents);
            })
        },
        function(best_contents, callback02) {
            insertBestContents(best_contents, function(err, result) {
                if ( err ) {
                    return callback02(err, null);
                } else {
                    if ( result.affectedRows == 0 ) {
                        return callback02(null, null);
                    }

                    return callback02(null, best_contents);
                }
            })
        },
        function(best_contents, callback03) {

            if ( !best_contents ) {
                return callback03(null, '데이터 없음');
            }
            updateBestContents(best_contents, function(err, result) {
                if ( err ) {
                    return callback03(err, null);
                } else {
                    return callback03(null, result);
                }
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

function getCurrentForceDate(best_type, fromdate, todate, callback) {

    let query = 
    `
    select ?                         fromdate
        ,  ?                         todate
        ,  date_format(?, '%Y%m%d')  best_id
        ,  date_format(?, '%H')      best_div
    `

    let parameter = [ fromdate, todate, todate, todate ];

    db.excuteSql(query, parameter, function (err, result){
        
        if (err) {
            console.log(' - [getCurrentDate] error : ' + err);
            return callback(err, null);
        } else {
            console.log(' - [getCurrentDate] compete');
            return callback(null, result);
        }
    });
}