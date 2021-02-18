let express = require('express');
let router = express.Router();
const commentaries = require('./commentaries.controller');

router.get('/byid/:mid', commentaries.converterCommentariesById);

module.exports = router;