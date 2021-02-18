let express = require('express');
let router = express.Router();
const standings = require('./standings.controller');

router.get('/', standings.convertStandings);

module.exports = router;