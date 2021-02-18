var mqtt = require("async-mqtt")
// function sendMQTT(matchid, match){
exports.sendMQTT = (topic, data) => {
    var mqtt = require("async-mqtt");

    var client  = mqtt.connect('mqtt://api.hifivefootball.com:4005')
    client.on("connect", send);

    async function send() {
    
        try {

            let jsonData = JSON.stringify(data)
            await client.publish(topic, jsonData)
    
            // This line doesn't run until the server responds to the publish 
            await client.end();
            // This line doesn't run until the client has disconnected without error 
        } catch (e){
            // Do something about it! 
            console.log('mqtt error : ' + e)
            await client.end();
        }
    }
}