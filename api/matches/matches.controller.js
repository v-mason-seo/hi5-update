const converter = require('../../converter_matches')

/**
 * 킥오프 4시간 전 부터 1시간 후의 매치 정보를 업데이트 한다.
 * @param {*} req 
 * @param {*} res 
 */
exports.convertLiveMatches = async (req, res) => {

    await converter.converterMatches()
                    .then(function() {
                        res.jsonp(200, { "msg" : "[convertLiveMatches] complete"});
                    })
                    .catch(function(err) {
                        res.jsonp(400, { "error" : "[convertLiveMatches] " + err })
                    });
}


/**
 * 기간으로 매치 데이터를 컨버트한다.
 *  - fromdate : 사직일자
 *  - todate   : 종료일자
 * @param {*} req 
 * @param {*} res 
 */
exports.convertPeriodMatches = async (req, res) => {

    let fromdate = req.query.fromdate;
    let todate = req.query.todate;

    if ( !fromdate) {
        return res.jsonp(400, { "msg" : "시작일자(fromdate)를 입력하세요" })
    }

    if ( !todate) {
        return res.jsonp(400, { "msg" : "종료일자(todate)를 입력하세요" })
    }

    await converter.converterMatchTodate(fromdate, todate)
                    .then(function() {
                        res.jsonp(200, { "msg" : "[convertPeriodMatches] complete"});
                    })
                    .catch(function(err) {
                        res.jsonp(400, { "error" : "[convertPeriodMatches] " + err })
                    });
}

/**
 * 매치 아이디로 매치 데이터를 컨버트한다.
 * @param {*} req 
 * @param {*} res 
 */
exports.convertMatchesById = async (req, res) => {

    let matchid = req.params.mid;

    if ( matchid ) {
        
        await converter.converterMatchToId(matchid)
        .then(function() {
            res.jsonp(200, { "msg" : "[convertMatchesById] complete"});
        })
        .catch(function(err) {
            res.jsonp(400, { "error" : "[convertMatchesById] " + err })
        });

    } else {
        return res.jsonp(400, { "msg" : "매치아이디를 입력하세요" })
    }
}