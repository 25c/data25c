var pg = require('pg').native;
var redis = require('redis-url');

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

function insertClick(result, callback) {
	var remove = result; 
	var data = JSON.parse(result);
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
					redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, remove, function(err, result) {
						if (err != null) {
							console.log("redis lrem error: " + err);
						}
						callback("not found user_uuid = " + data.user_uuid);
					});					
				} else if (result.rows.length == 1) {
					var user_id = result.rows[0].id;
					pgWebClient.query("SELECT id FROM buttons WHERE LOWER(uuid) = LOWER($1)", [ data.button_uuid ], function(err, result) {
						if (err != null) {
							console.log("could not find button");
							callback(err);
						} else if (result.rows.length == 0) {
							redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, remove, function(err, result) {
								if (err != null) {
									console.log("redis lrem error: " + err);
								}
								callback("not found button_uuid=" + data.button_uuid);
							});							
						} else if (result.rows.length == 1) {
							var button_id = result.rows[0].id;
							pg.connect(pgDataUrl, function(err, pgDataClient) {
								if (err != null) {
									console.log("Could not connect to data postgres: " + err);
									callback(err);
								} else {
									pgDataClient.query("INSERT INTO clicks (uuid, user_id, button_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", [data.uuid, user_id, button_id, data.ip_address, data.user_agent, data.referrer, 0, data.created_at, new Date()], function(err, result) {
										if (err != null) {
											if (err.routine = '_bt_check_unique') {
												redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, remove, function(err, result) {
													if (err != null) {
														console.log("redis lrem error: " + err);
													}
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
													console.log(data.uuid + ":" + data.created_at + ":" + user_id + ":" + button_id);
												}
												callback(null);
											});
										}
									});
								}
							});
						}
					});
				}
			});
		}
	});
}

function processQueue(err, result) {
	if (err != null) {
		console.log("redis brpoplpush error: " + err);
		redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
			processQueue(err, result);
		});
	} else {
		insertClick(result, function(err) {
			if (err != null) {
				console.log(err);
			}
			redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
				processQueue(err, result);
			});
		});
	}
}

console.log("Starting queue processing...");
redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
	processQueue(err, result);
});
