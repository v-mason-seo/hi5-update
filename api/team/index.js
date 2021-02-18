let express = require('express');
let router = express.Router();
const team = require('./team.controller');

console.log('== team.index ==');

router.get('/byid/:tid', team.convertTeamById);

module.exports = router;