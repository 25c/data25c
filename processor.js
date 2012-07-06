var pg = require('pg').native;
var redis = require('redis-url');

var pgDataUrl = process.env.DATABASE_URL;
if (pgDataUrl == undefined) {
	pgDataUrl = "tcp://localhost/data25c_development";
}

var pgWebUrl = process.env.DATABASE_WEB_URL;
if (pgWebUrl == undefined) {
	pgWebUrl = "tcp://localhost/web25c_development";
}

var QUEUE_KEY = 'QUEUE_DEDUCT';
var QUEUE_PROCESSING_KEY = 'QUEUE_DEDUCT_PROCESSING';
var redisDataClient = redis.connect(process.env.REDISTOGO_URL)

function changeClickState(data, funded, callback) {
	pg.connect(pgDataUrl, function(err, pgDataClient) {
		if (err != null) {
			console.log("Could not connect to data postgres: " + err);
			callback(err);
		} else {
			pgDataClient.query("BEGIN", function(err, result) {
				if (err != null) {
					console.log("Could not begin data transaction.");
					callback(err);
				} else {
					pgDataClient.query("SELECT state FROM clicks WHERE LOWER(uuid) = LOWER($1) FOR UPDATE", [ data.uuid ], function(err, result) {
						if (err != null) {
							console.log("Could not fetch click state");
							callback(err);
						} else if (result.rows.length == 1) {
							if (result.rows[0].state == 0) {
								pgDataClient.query("UPDATE clicks SET state=$1 WHERE LOWER(uuid) = LOWER($2)", [ funded ? 2 : 1, data.uuid ], function(err, result) {
									if (err != null) {
										console.log("Could not update click state");
										callback(err);
									} else {
										pgDataClient.query("PREPARE TRANSACTION 'click-" + data.uuid + "'", function(err, result) {
											if (err != null) {
												console.log("Could not prepare click state change transaction");
												callback(err);
											} else {
												pgDataClient.query("COMMIT PREPARED 'click-" + data.uuid + "'", function(err, result) {
													if (err != null) {
														console.log("Commit prepared failed");
														pgDataClient.query("ROLLBACK PREPARED 'click-" + data.uuid + "'", function() {
															callback(err);
														});
													} else {
														callback(null);
													}
												});
											}
										});
									}
								});
							} else {
								console.log("Could not find click " + data.uuid);
								callback("Could not find click " + data.uuid);
							}
						}
					});
				}
			});
		}
	});
}

function deductFromUserBalance(data, callback) {
	pg.connect(pgWebUrl, function(err, pgWebClient) {
		if (err != null) {
			console.log("Could not connect to web postgres: " + err);
			callback(err);
		} else {
			pgWebClient.query("BEGIN", function(err, result) {
				if (err != null) {
					console.log("Could not begin user transaction");
					callback(err);
				} else {
					pgWebClient.query("SELECT balance FROM users WHERE LOWER(uuid) = LOWER($1) FOR UPDATE", [ data.user_uuid ], function(err, result) {
						if (err != null) {
							console.log("Could not get user balance");
							callback(err);
						} else if (result.rows.length == 1) {
							var balance = result.rows[0].balance;
							pgWebClient.query("UPDATE users SET balance=$1 WHERE LOWER(uuid) = LOWER($2)", [ balance-1, data.user_uuid ], function(err, result) {
								if (err != null) {
									console.log("Could not deduct from user balance");
									callback(err);
								} else {
									pgWebClient.query("PREPARE TRANSACTION 'user-" + data.uuid + "'", function(err, result) {
										if (err != null) {
											console.log("Could not prepare balance deduction");
											callback(err);
										} else {
											changeClickState(data, balance > 0, function(err) {
												if (err != null) {
													pgWebClient.query("ROLLBACK PREPARED 'user-" + data.uuid + "'", function() {
														callback(err);
													});
												} else {
													pgWebClient.query("COMMIT PREPARED 'user-" + data.uuid + "'", function(err, result) {
														if (err != null) {
															console.log("CRITICAL ERROR user commit failed");
															callback(err);
														} else {
															console.log("DONE: " + data.uuid);
															callback(null);
														}
													});
												}
											});
										}
									});			
								}
							});
						} else {
							console.log("User not found " + data.user_uuid);
							callback("User not found " + data.user_uuid);
						}
					});
				}
			});
		}
	});
}

function processQueue(err, result) {
	var remove = result; 
	if (err != null) {
		console.log("redis brpoplpush error: " + err);
		redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
			processQueue(err, result);
		});
	} else {
		var data = JSON.parse(result);
		console.log("Processing: " + data.uuid);
		deductFromUserBalance(data, function(err) {
			if (err != null) {
				console.log("ERROR: " + data.uuid + ": " + err);
				redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
					processQueue(err, result);
				});
			} else {
				redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, remove, function(err, result) {
					if (err != null) {
						console.log("redis lrem error: " + err);
					}
					redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
						processQueue(err, result);
					});
				});
			}
		});
	}
}

console.log("Starting deduct queue processing...");
redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
	processQueue(err, result);
});
