const converter = require('../../converter_teams')


exports.convertTeamById = async (req, res) => {

    console.log('call convertTeamById');

    let team_fa_id = req.params.tid;

    if ( !team_fa_id) {
        return res.jsonp(400, { "msg" : "팀 아이디(tid) 값을 입력하세요" })
    }

    //  await converter.convertTeamToId(team_fa_id)
    //                 .then(function()     { res.jsonp(200, { "msg" : "complete"}) })
    //                 .catch(function(err) { res.jsonp(400, { "msg" : err })       });

    await converter.convertTeamToId(team_fa_id);

    //res.jsonp(200, { "msg" : "complete"});
    return res.status(200).json( { "msg" : "complete1"} )
}


