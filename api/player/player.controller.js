const converter = require('../../converter_players')


exports.convertPlayerById = async (req, res) => {

    let player_fa_id = req.params.pid;

    if ( !player_fa_id) {
        return res.jsonp(400, { "msg" : "플레이어 아이디(pid) 값을 입력하세요" })
    }

     await converter.converterPlayerToId(player_fa_id)
                    .then(function()     { res.jsonp(200, { "msg" : "[convertPlayerById] complete"}) })
                    .catch(function(err) { res.jsonp(400, { "msg" : "[convertPlayerById]" + err })       });
}


exports.convertPlayerBySql = async (req, res) => {

    let query = req.query.sql;

    if ( !query) {
        return res.jsonp(400, { "msg" : "sql를 입력하세요(player_fa_id는 반드시 포함되어야함)" })
    }

     await converter.converterPlayerbySql(query)
                    .then(function()     { res.jsonp(200, { "msg" : "[convertPlayerById] complete"}) })
                    .catch(function(err) { res.jsonp(400, { "msg" : "[convertPlayerById]" + err })       });
}


