const converter = require('../../converter_standings')


exports.convertStandings = async (req, res) => {

    let comp_fa_id = req.query.comp_id;

    if ( comp_fa_id) {
        //
        // 1. 모든 리그 업데이트
        //
        await converter.converterStandings()
                        .then(function()     { res.jsonp(200, { "msg" : "complete"}) })
                        .catch(function(err) { res.jsonp(400, { "msg" : err })       });
    } else {
        //
        // 2. comp_fa_id 리그만 업데이트
        //
        await converter.converterStandingToId(comp_fa_id)
                        .then(function()     { res.jsonp(200, { "msg" : "complete"}) })
                        .catch(function(err) { res.jsonp(400, { "msg" : err })       });
    }
}


