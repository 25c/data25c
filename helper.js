var pg = require('pg').native;
var redis = require('redis-url');
var airbrake = require('airbrake').createClient('25f60a0bcd9cc454806be6824028a900');
airbrake.developmentEnvironments = ['development'];
airbrake.handleExceptions();

var QUEUE_KEY = 'QUEUE';
var PROCESSING_KEY = 'QUEUE_PROCESSING';
var redisDataClient = redis.connect(process.env.REDISTOGO_URL)

var pgDataUrl = process.env.DATABASE_URL;
if (pgDataUrl == undefined) {
	pgDataUrl = "tcp://localhost/data25c_development";
}
var pgWebUrl = process.env.DATABASE_WEB_URL;
if (pgWebUrl == undefined) {
	pgWebUrl = "tcp://localhost/web25c_development";
}

function processReQueue(err1, result1) {
	if (err1 != null) {
		console.log("redis lindex error: " + err1);
		airbrake.notify(err1);
	}
	setTimeout(function(){
		redisDataClient.lindex(PROCESSING_KEY, -1, function(err2, result2) {
			if (err2 != null) {
				console.log("redis lindex error: " + err2);
				airbrake.notify(err2);
			} else if (result1 != null && result2 != null) {
				var data1 = JSON.parse(result1);
				var data2 = JSON.parse(result2);
				if (data1.uuid == data2.uuid) {
					redisDataClient.brpoplpush(PROCESSING_KEY, QUEUE_KEY, 0, function(err, result) {
						if (err != null) {
							console.log("redis brpoplpush error: " + err);
							airbrake.notify(err);
						}
					});
				} 
			}
			processReQueue(err2, result2);
		});
	}, 500);
}

console.log("Secondary queue processing...");
redisDataClient.lindex(PROCESSING_KEY, -1, function(err, result) {
	processReQueue(err, result);
});
