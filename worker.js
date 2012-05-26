var pg = require('pg');
var redis = require('redis-url');

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

function processQueue(pgDataClient, pgWebClient, err, result) {
	data = JSON.parse(result);
	pgWebClient.query("SELECT id FROM users WHERE LOWER(uuid)=LOWER($1)", [ data.user_uuid ], function(err, result) {
		if (err != null) {
			console.log(err);
		} else if (result.rowCount == 1) {
			var user_id = result.rows[0].id;
			pgWebClient.query("SELECT id FROM users WHERE LOWER(uuid)=LOWER($1)", [ data.publisher_uuid ], function(err, result) {
				if (err != null) {
					console.log(err);
				} else if (result.rowCount == 1) {
					var publisher_user_id = result.rows[0].id;
					if (data.content_uuid) {
						pgWebClient.query("SELECT id, user_id FROM contents WHERE LOWER(uuid)=LOWER($1)", [ data.content_uuid ], function(err, result) {
							if (err != null) {
								console.log(err);
							} else if (result.rowCount == 1) {
								if (result.rows[0].user_id == publisher_user_id) {
									var content_id = result.rows[0].id;
									pgDataClient.query("INSERT INTO clicks (user_id, publisher_user_id, content_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", [user_id, publisher_user_id, content_id, data.ip_address, data.user_agent, data.referrer, 0, data.created_at, new Date()], function(err, result) {
										if (err != null) {
											console.log(err);
										} else {
											console.log(data.created_at + ":" + user_id + ":" + publisher_user_id + ":" + content_id);
										}
									});
								}
							}
						});
					} else {
						pgDataClient.query("INSERT INTO clicks (user_id, publisher_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", [user_id, publisher_user_id, data.ip_address, data.user_agent, data.referrer, 0, data.created_at, new Date()], function(err, result) {
							if (err != null) {
								console.log(err);
							} else {
								console.log(data.created_at + ":" + user_id + ":" + publisher_user_id);
							}
						});
					}
				}
			});
		}
	});
	redisDataClient.brpoplpush(QUEUE_KEY, PROCESSING_KEY, 0, function(err, result) {
		processQueue(pgDataClient, pgWebClient, err, result);
	})
}

pg.connect(pgDataUrl, function(err, pgDataClient) {
	if (err == null) {
		pg.connect(pgWebUrl, function(err, pgWebClient) {
			redisDataClient.brpoplpush(QUEUE_KEY, PROCESSING_KEY, 0, function(err, result) {
				processQueue(pgDataClient, pgWebClient, err, result);
			})
		});
	}
});
