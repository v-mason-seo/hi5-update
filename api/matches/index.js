let express = require('express');
let router = express.Router();
const matches = require('./matches.controller');


/**
 * 킥오프 4시간 전 부터 1시간 후의 매치 정보를 업데이트 한다.
 */
router.get('/live', matches.convertLiveMatches);


/**
 * 기간으로 매치 데이터를 컨버트한다.
 *   - fromdate : 사직일자
 *   - todate   : 종료일자
 */
router.get('/period', matches.convertPeriodMatches);


/**
 * 매치 아이디로 컨버트한다.
 */
router.get('/byid/:mid', matches.convertMatchesById);


module.exports = router;