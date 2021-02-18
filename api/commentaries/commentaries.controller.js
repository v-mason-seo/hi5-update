const converter = require('../../converter_commentaries')


exports.converterCommentariesById = async (req, res) => {

    let match_fa_id = req.params.mid;

    if ( !match_fa_id) {
        return res.jsonp(400, { "msg" : "매치 아이디(mid) 값을 입력하세요" })
    }

    converter.converterCommentariesToId(match_fa_id);

    res.jsonp(200, { "msg" : "complete"});
}


