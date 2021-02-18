let express = require('express');
let router = express.Router();
const player = require('./player.controller');

router.get('/byid/:pid', player.convertPlayerById);

router.get('/bySql', player.convertPlayerBySql);


module.exports = router;