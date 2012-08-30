var pg = require('pg').native;
var redis = require('redis-url');
var airbrake = require('airbrake').createClient('25f60a0bcd9cc454806be6824028a900');
airbrake.developmentEnvironments = ['development'];
airbrake.handleExceptions();

var QUEUE_KEY = 'QUEUE';
var QUEUE_PROCESSING_KEY = 'QUEUE_PROCESSING';
var QUEUE_NEXT_KEY = 'QUEUE_DEDUCT';
var redisDataClient = redis.connect(process.env.REDISTOGO_URL)

var pgDataUrl = process.env.DATABASE_URL;
if (pgDataUrl == undefined) {
	pgDataUrl = "tcp://localhost/data25c_development";
}
var pgWebUrl = process.env.DATABASE_WEB_URL;
if (pgWebUrl == undefined) {
	pgWebUrl = "tcp://localhost/web25c_development";
}

function removeEntry(entry, callback) {
	redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, entry, function(err, result) {
		if (err != null) {
			console.log("redis lrem error: " + err);
			airbrake.notify(err);
		}
		callback();
	});
}

function insertClick(uuid, user_id, button_id, referrer_user_id, ip_address, user_agent, referrer, created_at, remove, callback) {
	pg.connect(pgDataUrl, function(err, pgDataClient) {
		if (err != null) {
			console.log("Could not connect to data postgres: " + err);
			callback(err);
		} else {
			pgDataClient.query("INSERT INTO clicks (uuid, user_id, button_id, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", [uuid, user_id, button_id, referrer_user_id, ip_address, user_agent, referrer, 0, created_at, new Date()], function(err, result) {    	  
				if (err != null) {
					if (err.routine = '_bt_check_unique') {
						removeEntry(remove, function() {
							callback(null);
						});
					} else {
						console.log("Not a uniqueness " + err);
						callback(err);
					}
				} else {
					redisDataClient.multi().lrem(QUEUE_PROCESSING_KEY, 0, remove).lpush(QUEUE_NEXT_KEY, remove).exec(function(err, result) {
						if (err != null) {
							console.log("redis lrem/lpush error: " + err);
						}	else {
							console.log(uuid + ":" +created_at + ":" + user_id + ":" + button_id);
						}
						callback(null);
					});
				}
			});
		}
	});
}

function validateClick(result, callback) {
	var remove = result; 
	var data = JSON.parse(result);
	if (data == null) {
		removeEntry(remove, function() {
			callback("Could not parse result: " + result);
		});
		return
	}
	console.log("Processing: " + data.uuid);
	pg.connect(pgWebUrl, function(err, pgWebClient) {
		if (err != null) {
			console.log("Could not connect to web postgres: " + err);
			callback(err);
		} else {
			pgWebClient.query("SELECT id FROM users WHERE LOWER(uuid) = LOWER($1)", [ data.user_uuid ], function(err, result) {
				if (err != null) {
					console.log("could not query for user_uuid: " + err);
					callback(err);
				} else if (result.rows.length == 0) {
					removeEntry(remove, function() {
						callback("not found user_uuid = " + data.user_uuid);
					});
				} else if (result.rows.length == 1) {
					var user_id = result.rows[0].id;
					pgWebClient.query("SELECT id, user_id FROM buttons WHERE LOWER(uuid) = LOWER($1)", [ data.button_uuid ], function(err, result) {
						if (err != null) {
							console.log("could not find button");
							callback(err);
						} else if (result.rows.length == 0) {
							removeEntry(remove, function() {
								callback("not found button_uuid=" + data.button_uuid);
							});
						} else if (result.rows.length == 1) {
							var button_id = result.rows[0].id;
							var button_user_id = result.rows[0].user_id;
							
							if (user_id != button_user_id) {
  							if (data.referrer_user_uuid) {
  							  pgWebClient.query("SELECT id FROM users WHERE LOWER(uuid) = LOWER($1)", [ data.referrer_user_uuid ], function(err, result) {
            				if (err != null) {
            					console.log("could not query for referrer_user_uuid: " + err);
            					callback(err);
            				} else if (result.rows.length == 0) {
          						console.log("not found referrer_user_uuid = " + data.referrer_user_uuid);
  							      insertClick(data.uuid, user_id, button_id, null, data.ip_address, data.user_agent, data.referrer, data.created_at, remove, callback);
            				} else if (result.rows.length == 1) {
            					var referrer_user_id = result.rows[0].id;
  							      insertClick(data.uuid, user_id, button_id, referrer_user_id, data.ip_address, data.user_agent, data.referrer, data.created_at, remove, callback);
  						      }
  						    });
  							} else {
  							  insertClick(data.uuid, user_id, button_id, null, data.ip_address, data.user_agent, data.referrer, data.created_at, remove, callback);
  							}
							} else {
                  counterKey = data.user_uuid + ":" + data.button_uuid;
                  redisDataClient.multi().lrem(QUEUE_PROCESSING_KEY, 0, remove).set(counterKey, '0').exec(function(err, result) {
                     if (err != null) {
                       console.log("redis lrem/lpush error: " + err);
                     } else {
                       console.log("User and button are the same, dropping");
                     }
                   callback(null);
                });
                callback(null);
						  }
					  }
					});
				}
			});
		}
	});
}

function processQueue() {
	redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
		if (err != null) {
			console.log("redis brpoplpush error: " + err);
			airbrake.notify(err);
			processQueue();
		} else {
			validateClick(result, function(err) {
				if (err != null) {
					console.log(err);
					airbrake.notify(err);
				}
				processQueue();  	
			});
		}
	});
}

console.log("Starting queue processing...");
processQueue();
