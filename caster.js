const request = require('request')


const baseURL = "http://api.hifivefootball.com:5550/reaction/match/"
// const baseURL = "http://127.0.0.1:5550/reaction/"

exports.sendMatchUpdated = (matchid ) => {

    let url = baseURL + matchid;
    
    request.post(url , function (error, response, body) {
        // console.log('error:', error); // Print the error if one occurred
        // console.log('statusCode:', response && response.statusCode); // Print the response status code if a response was received
        // console.log('body:', body); // Print the HTML for the Google homepage.
      });    
}

exports.sendMatchEvent = function() {

    let url = "http://api.hifivefootball.com:5550/" + "match/events";

    request.get(url, function(err, response, body) {
        console.log("sendMatchEvent complete");
    });
}

