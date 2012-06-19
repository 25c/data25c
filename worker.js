var pg = require('pg').native;
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
			console.log("could not query for user_uuid: " + err);
		} else if (result.rows.length == 0) {
			console.log("not found user_uuid=" + data.user_uuid);
		} else if (result.rows.length == 1) {
			var user_id = result.rows[0].id;
			pgWebClient.query("SELECT id FROM buttons WHERE LOWER(uuid)=LOWER($1)", [ data.button_uuid ], function(err, result) {
				if (err != null) {
					console.log(err);
				} else if (result.rows.length == 0) {
					console.log("not found button_uuid=" + data.button_uuid);
				} else if (result.rows.length == 1) {
					var button_id = result.rows[0].id;
					pgDataClient.query("INSERT INTO clicks (uuid, user_id, button_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", [data.uuid, user_id, button_id, data.ip_address, data.user_agent, data.referrer, 0, data.created_at, new Date()], function(err, result) {
						if (err != null) {
							console.log(err);
						} else {
							console.log(data.created_at + ":" + user_id + ":" + button_id);
						}
					});
				}
			});
		}
	});
	redisDataClient.brpoplpush(QUEUE_KEY, PROCESSING_KEY, 0, function(err, result) {
		processQueue(pgDataClient, pgWebClient, err, result);
	})
}

pg.connect(pgDataUrl, function(err, pgDataClient) {
	if (err != null) {
		console.log("Could not connect to data postgres: " + err);
	} else {
		pg.connect(pgWebUrl, function(err, pgWebClient) {
			if (err != null) {
				console.log("Could not connect to web postgres: " + err);
			} else {
				console.log("Starting queue processing...");
				redisDataClient.brpoplpush(QUEUE_KEY, PROCESSING_KEY, 0, function(err, result) {
					processQueue(pgDataClient, pgWebClient, err, result);
				})
			}
		});
	}
});
