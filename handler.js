module.exports.caller = function(event, context, callback) {
    
console.log("Function started");

//HTTP web server infrastructure libraries
var request = require("request");

//Moment JS
var moment = require('moment');

//local keys
var ENV = require('./keys');
     
//NodeJS - Postgres driver: https://github.com/brianc/node-postgres
var {Pool, Client} = require("pg");
const pool = new Pool({
  user: ENV.DB_USER,
  host: ENV.DB_ENDPOINT,
  database: ENV.DB_DEFAULT,
  password: ENV.DB_PW,
  port: ENV.DB_PORT
});

var pgClient = new Client({ //instantise pool connection
  user: ENV.DB_USER,
  host: ENV.DB_ENDPOINT,
  database: ENV.DB_DEFAULT,
  password: ENV.DB_PW,
  port: ENV.DB_PORT
});

//ORM facilitating JSON to SQL
var jsonSql = require('json-sql')({
    separatedValues: false,
    namedValues: false,
});
    
//Mailgun - for auto email sendouts
var mailgun = require('mailgun-js')({
        apiKey: ENV.MG_APIKEY, 
        domain: ENV.MG_DOMAIN
    });

//Finance API caller
var Caller = {    
    getCurrency: function() {
        var url = "https://www.alphavantage.co/query?"
            + "function=FX_INTRADAY"
            + "&from_symbol=AUD"
            + "&to_symbol=USD"
            + "&interval=60min"
            + "&outputsize=compact"
            + "&apikey="
            + ENV.AV_APIKEY;      
           
        // Return new promise 
        return new Promise(function(resolve, reject) {
            // Do async job
            request.get(url, function(err, resp, body) {
                if (err) {
                    reject(err);
                } else {
                    var json = JSON.parse(body);
                    var output = {
                        metaData: json["Meta Data"],
                        content: json["Time Series FX (60min)"]
                    };   
                    resolve(output);
                }
            });
        });
    },
    
    getIndex: function() {
        var url = "https://www.alphavantage.co/query?"
            + "function=TIME_SERIES_INTRADAY"
            + "&symbol=DJI"
            + "&interval=60min"
            + "&outputsize=compact"
            + "&apikey="
            + ENV.AV_APIKEY;

        // Return new promise 
        return new Promise(function(resolve, reject) {
            // Do async job
            request.get(url, function(err, resp, body) {
                if (err) {
                    reject(err);
                } else {
                    var json = JSON.parse(body);
                    var output = {
                        metaData: json["Meta Data"],
                        content: json["Time Series (60min)"]
                    };   
                    resolve(output);
                }
            });
        });
        
    }    
};
    
/*
 * Postgres database pusher
 * Mailgun pusher
 */
var Pusher = {
    pushDatabase: function(SQLquery) {
        pool.query(SQLquery, (err, res) => {
          if (err) {
            console.log(err);
            callback("Error - please see log"); 
         }
        });
    },
    sendEmail: function() {    
        var data = {
        	from: 'Administrator <admin@aycholdings.com.au>',
        	to: ENV.ADMIN_EMAIL,
        	subject: 'Success ETL',
        	text: 'Dear Sir/Madam, please be advised ETL has been successful. Regards, Admin.'
        };
        mailgun
            .messages()
            .send(data, function (error, body) {
            	if (body) console.log("Successful email sent");
            });
    }
};
//Get Currency data and push to database
Caller.getCurrency().then(
    function(data) {       
        console.log("Successful download of currency");
        var sqlQuery = '';
        for (pty in data.content) {
            var sql = jsonSql.build({
                type: 'insert',
                table: 'aud_test',
                values: {
                    timestamp: pty,
                    open: data.content[pty]["1. open"],
                    high: data.content[pty]["2. high"],
                    low: data.content[pty]["3. low"],
                    close: data.content[pty]["4. close"],
                    createdAt: moment().format()
                },
            }); 
            sqlQuery += sql.query;
        }
        Pusher.pushDatabase(sqlQuery);//Run SQL query
        Pusher.sendEmail(); //Mailgun email to admin          
        console.log("Success currency");
    }, 
    function(err) {
        console.log(err);
    }
);
//Get DJX index data and push to database
Caller.getIndex().then(
    function(data) {
        console.log("Successful download of DJX");
        var sqlQuery = '';
        for (pty in data.content) {
            var sql = jsonSql.build({
                type: 'insert',
                table: 'djx_test',
                values: {
                    timestamp: pty,
                    open: data.content[pty]["1. open"],
                    high: data.content[pty]["2. high"],
                    low: data.content[pty]["3. low"],
                    close: data.content[pty]["4. close"],
                    volume: data.content[pty]["5. volume"],
                    createdAt: moment().format()
                },
            }); 
            sqlQuery += sql.query;
        }  
        Pusher.pushDatabase(sqlQuery); //Run SQL query  
        Pusher.sendEmail(); //Mailgun email to admin           
        console.log("Success DJX");
    }, 
    function(err) {
        console.log(err);
    }
);    

callback(null, "Success");       
};
