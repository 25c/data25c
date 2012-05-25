var pg = require('pg');
var redis = require('redis-url');

var redisDataClient = redis.connect(process.env.REDISTOGO_URL)

var pgUrl = process.env.DATABASE_URL;
if (pgUrl == undefined) {
	
}
pg.connect(process.env.DATABASE_URL, function(err, client) {
	if (err == null) {
		for(;;) {
		
		}
	}
});
