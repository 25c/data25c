var pg = require('pg').native;
var redis = require('redis-url');
var airbrake = require('airbrake').createClient('25f60a0bcd9cc454806be6824028a900');
airbrake.developmentEnvironments = ['development'];
airbrake.handleExceptions();

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
var redisDataClient = redis.connect(process.env.REDISTOGO_URL);

var fs = require('fs');
var nodemailer = require("nodemailer");
var EMAIL_SERVICE = "SendGrid";
var EMAIL_USERNAME = "corp25c";
var EMAIL_PASSWORD = "sup3rl!k3";
var EMAIL_FROM = "no-reply@25c.com";
var smtpTransport = nodemailer.createTransport("SMTP", {
    service: EMAIL_SERVICE,
    auth: {
        user: EMAIL_USERNAME,
        pass: EMAIL_PASSWORD
    }
});

function removeEntry(entry, callback) {
	redisDataClient.lrem(QUEUE_PROCESSING_KEY, 0, entry, function(err, result) {
		if (err != null) {
			console.log("redis lrem error: " + err);
			airbrake.notify(err);
		}
		callback();
	});
}

function changeClickState(data, state, callback) {
	pg.connect(pgDataUrl, function(err, pgDataClient) {
		if (err != null) {
			console.log("Could not connect to data postgres: " + err);
			airbrake.notify(err);
			callback(err);
		} else {
			pgDataClient.query("BEGIN", function(err, result) {
				if (err != null) {
					console.log("Could not begin data transaction.");
					airbrake.notify(err);
					callback(err);
				} else {
					pgDataClient.query("SELECT state FROM clicks WHERE LOWER(uuid) = LOWER($1) FOR UPDATE", [ data.uuid ], function(err, result) {
						if (err != null) {
							console.log("Could not fetch click state");
							airbrake.notify(err);
							callback(err);
						} else if (result.rows.length == 1) {
							if (result.rows[0].state == 0) {
								pgDataClient.query("UPDATE clicks SET state=$1 WHERE LOWER(uuid) = LOWER($2)", [ state, data.uuid ], function(err, result) {
									if (err != null) {
										console.log("Could not update click state");
										airbrake.notify(err);
										callback(err);
									} else {
										pgDataClient.query("PREPARE TRANSACTION 'click-" + data.uuid + "'", function(err, result) {
											if (err != null) {
												console.log("Could not prepare click state change transaction");
												airbrake.notify(err);
												callback(err);
											} else {
												pgDataClient.query("COMMIT PREPARED 'click-" + data.uuid + "'", function(err, result) {
													if (err != null) {
														console.log("Commit prepared failed");
														airbrake.notify(err);
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
								callback("Click state already=" + result.rows[0].state);
							}
						} else {
							callback("Could not find click " + data.uuid);
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
			airbrake.notify(err);
			callback(err);
		} else {
			pgWebClient.query("BEGIN", function(err, result) {
				if (err != null) {
					console.log("Could not begin user transaction");
					airbrake.notify(err);
					callback(err);
				} else {
					pgWebClient.query("SELECT balance FROM users WHERE LOWER(uuid) = LOWER($1) FOR UPDATE", [ data.user_uuid ], function(err, result) {
						if (err != null) {
							console.log("Could not get user balance");
							airbrake.notify(err);
							callback(err);
						} else if (result.rows.length == 1) {
							var balance = result.rows[0].balance;
							balance = balance - 1;
							if (balance > -40) {
  							pgWebClient.query("UPDATE users SET balance=$1 WHERE LOWER(uuid) = LOWER($2)", [ balance, data.user_uuid ], function(err, result) {
  								if (err != null) {
  									console.log("Could not deduct from user balance");
  									airbrake.notify(err);
  									callback(err);
  								} else {
  									pgWebClient.query("PREPARE TRANSACTION 'user-" + data.uuid + "'", function(err, result) {
  										if (err != null) {
  											console.log("Could not prepare balance deduction");
  											airbrake.notify(err);
  											callback(err);
  										} else {
  											changeClickState(data, 1, function(err) {
  												if (err != null) {
  													pgWebClient.query("ROLLBACK PREPARED 'user-" + data.uuid + "'", function() {
  														callback(err);
  													});
  												} else {
  													pgWebClient.query("COMMIT PREPARED 'user-" + data.uuid + "'", function(err, result) {
  														if (err != null) {
  															console.log("CRITICAL ERROR user commit failed");
  															airbrake.notify(err);
  															callback(err);
  														} else {
  															//// update balance cache in redis
  															redisDataClient.set("user:" + data.user_uuid, balance, function(err, result) {
  																if (err != null) {
  																	console.log(err);
  																	airbrake.notify(err);
  																} else {												  
                                    // do something
  															  }
  															})
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
							  callback("User overdraft: " + data.user_uuid);
						  }
						} else {
							console.log("User not found: " + data.user_uuid);
							callback("User not found: " + data.user_uuid);
						}
					});
				}
			});
		}
	});
}

function sendEmail(to, filename, args) {
  fs.readFile(filename, "utf8", function(err, data) {
    if (err) {
      "Error reading email file: " + console.log(err);
    } else {
      subject = data.split("#{", 2)[1].split("}", 1)[0];
      body = data.substring(data.indexOf("}")).replace(/^\s\s*/, '').replace(/\s\s*$/, '');
      
      parts = body.split("#{");
      for (key in args) {
        for (i = 1; i < parts.length; i++) {
          if (parts[i].split("}", 1)[0].indexOf(key) != -1) {
            parts[i] = parts[i].replace(/.*}/, args[key]);
          }
        }
      }
      if (parts.length > 1) {
        parts[0] = parts[0].replace(/.*}\s*/, '');
        body = parts.join("");
      }
      var mailOptions = {
        from: EMAIL_FROM,
        to: to,
        subject: subject,
        html: body,
        generateTextFromHTML: true
      };
      smtpTransport.sendMail(mailOptions, function(err, response) {
        if(err){
          console.log("Could not send email: " + err);
        }
      });
    }
  });
}

function sendOverdraftEmail(uuid) {
  pg.connect(pgWebUrl, function(err, pgWebClient) {
		if (err != null) {
			console.log("Could not connect to web postgres: " + err);
			airbrake.notify(err);
			callback(err);
		} else {
      pgWebClient.query("SELECT email, first_name, nickname FROM users WHERE uuid = LOWER($1)", [ uuid ], function(err, result) {
    	  if (err != null) {
    	    console.log("Getting user email error: " + err);
        } else if (result.rows[0] == undefined) {
          console.log("User not found!");
        } else if (!result.rows[0].email) {
          console.log("User does not have an email address!");
        } else {
          userEmail = result.rows[0].email;
          userFirstName = result.rows[0].first_name;
          userNickname = result.rows[0].nickname;
      
          toName = userFirstName || userNickname || userEmail;
    		  sendEmail(userEmail, "overdraft_email.txt", {name: toName});
        }
      });
    }
  });
}

function processQueue(err, result) {
	if (err != null) {
		console.log("redis brpoplpush error: " + err);
		airbrake.notify(err);
		redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
			processQueue(err, result);
		});
	} else {
		var data = JSON.parse(result);
		if (data == null) {
			console.log("Could not parse result=" + result);
			removeEntry(result, function() {
				redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
					processQueue(err, result);
				});
			});
			return;
		}
		console.log("Processing: " + data.uuid);
		deductFromUserBalance(data, function(err) {
			if (err != null) {
				console.log("ERROR: " + data.uuid + ": " + err);
				airbrake.notify(err);
			}
			removeEntry(result, function() {
				redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
					processQueue(err, result);
				});
			});
		});
	}
}

console.log("Starting deduct queue processing...");
redisDataClient.brpoplpush(QUEUE_KEY, QUEUE_PROCESSING_KEY, 0, function(err, result) {
	processQueue(err, result);
});
