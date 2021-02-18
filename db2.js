const appconfig = require('./appconfig');
const  mysql = require('mysql2/promise');

var pool  = mysql.createPool({
  host     : appconfig.host,
  user     : appconfig.user,
  password : appconfig.password,
  database : appconfig.database ,
  port     : appconfig.port,
  connectionLimit:5,
  dateStrings : true,
  multipleStatements: true
});

exports.getQuery = async function(sql, params) {

    var conn = await pool.getConnection();
    var result = await conn.execute(sql, params);
    conn.release();

    return result;

}

exports.excuteSql = function( sql, parameter,  callback){
    var sql;
    pool.getConnection( function( err, connection){
        if (err) return callback(err, null)
        var q = connection.query( sql, parameter, function( err, results, fields) {
            connection.release();
            sql = q.sql;
            
            if (err) {
                 return callback(err,null)
            }

            return callback(null, results);
        });
        
    });
};

exports.insertLog = function(log, callback) {
    
    if ( !log ) {
        return callback(err, 'undefined log');
    }

    let query =
    `
    INSERT INTO convert_log (code, subcode, errcode, msg) 
    VALUES ( ?, ?, ?, ? ); 
    `

    let parameter = [log.code, log.subcode, log.errcode, log.msg]

    // excuteSql(query, paramater, (err, result)=>{
    //     if (err) {
    //         callback(err, null);
    //     } else {
    //         callback(null, result);
    //     }
    // })

    pool.getConnection( function( err, connection){
        if (err) 
            return callback(err, null)
            
        var q = connection.query( query, parameter,  function( err, results, fields ) {
            connection.release();
            if (err){ 
                return callback(err,null)
            }

            return callback(null, results);
        });
    });
}
