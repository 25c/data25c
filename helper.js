var pg = require('pg').native;
var redis = require('redis-url');
var airbrake = require('airbrake').createClient('25f60a0bcd9cc454806be6824028a900');
airbrake.developmentEnvironments = ['development'];
airbrake.handleExceptions();

var QUEUE_KEY = 'QUEUE';
var QUEUE_PROCESSING_KEY = 'QUEUE_PROCESSING';
var QUEUE_DEDUCT_KEY = 'QUEUE_DEDUCT';
var QUEUE_DEDUCT_PROCESSING_KEY = 'QUEUE_DEDUCT_PROCESSING';
var redisDataClient = redis.connect(process.env.REDISTOGO_URL)

var pgDataUrl = process.env.DATABASE_URL;
if (pgDataUrl == undefined) {
	pgDataUrl = "tcp://localhost/data25c_development";
}
var pgWebUrl = process.env.DATABASE_WEB_URL;
if (pgWebUrl == undefined) {
	pgWebUrl = "tcp://localhost/web25c_development";
}

function compareResults(queueKey, queueProcessingKey, previousResult) {
	//// wait to check again after delay
	setTimeout(function() {
		redisDataClient.lindex(queueProcessingKey, -1, function(err, result) {
			if (err != null) {
				//// an error occurred, log
				console.log(err);
				airbrake.notify(err);
				//// try comparison again
				compareResults(queueKey, queueProcessingKey, previousResult);
			} else {
				if (result == previousResult) {
					//// still around, so remove from processing queue and re-enqueue onto main queue
					console.log("Re-enqueue into " + queueKey + ": " + result);
					redisDataClient.multi().lrem(queueProcessingKey, 0, result).lpush(queueKey, result).exec(function(err, result) {
						if (err != null) {
							console.log(err);
							airbrake.notify(err);
						}
						//// start this process again
						processQueue(queueKey, queueProcessingKey);
					});
				} else {
					//// compare again with this new result
					compareResults(queueKey, queueProcessingKey, result);
				}
			}
		});		
	}, 500);
}

function processQueue(queueKey, queueProcessingKey) {
	//// peek at the head of the processing queue
	redisDataClient.lindex(queueProcessingKey, -1, function(err, result) {
		if (err != null) {
			//// an error occurred, log
			console.log(err);
			airbrake.notify(err);
			//// start over after a delay
			setTimeout(function() {
				processQueue(queueKey, queueProcessingKey);
			}, 500);
		} else {
			//// compare and re-enqueue if necessary
			if (result != null) {
				compareResults(queueKey, queueProcessingKey, result);
			}
		}
	});	
}

processQueue(QUEUE_KEY, QUEUE_PROCESSING_KEY);
processQueue(QUEUE_DEDUCT_KEY, QUEUE_DEDUCT_PROCESSING_KEY);
