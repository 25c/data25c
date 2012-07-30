var pg = require('pg').native;
var redis = require('redis-url');
var prompt = require('prompt');

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

function resetUser(user_email) {

	pg.connect(pgWebUrl, function(err, pgWebClient) {
		if (err != null) {
			console.log("Could not connect to web postgres: " + err);
			callback(err);
		} else {
      pgWebClient.query("SELECT id, uuid FROM users WHERE email = LOWER($1)", [ user_email ], function(err, result) {
    	  if (err != null) {
    	    console.log("setting pg user balance error: " + err);
        } else if (result.rows[0] == undefined) {
          console.log("user not found!");
        } else {
          user_id = result.rows[0].id;
          user_uuid = result.rows[0].uuid;
          resetButtons(user_uuid, user_id);
        }
      });
      
      pgWebClient.query("UPDATE users SET balance = 0 WHERE email = LOWER($1)", [ user_email ], function(err, result) {
        if (err != null) {
          console.log("setting pg user balance error: " + err);
        }
      });
    }
  });
}

function resetButtons(user_uuid, user_id) {
  pg.connect(pgDataUrl, function(err, pgDataClient) {
  if (err != null) {
  	console.log("Could not connect to web postgres: " + err);
  	callback(err);
  } else {
    pgDataClient.query("UPDATE clicks SET state = 6 WHERE user_id = $1", [ user_id ], function(err, result) {
      if (err != null) {
        console.log("setting user clicks to dropped error: " + err);
        }
      });

    pgDataClient.query("SELECT button_id FROM clicks WHERE user_id = $1", [ user_id ], function(err, result) {
      if (err != null) {
        console.log("finding button ids user has clicked on error: " + err);
        } else {
         if (result == null) {
           console.log("no button id data returned!");
           return;
         }
         var button_ids = [];
         for (row in result.rows) {
           var button_id = result.rows[row].button_id;
           var alreadyHasId = false;
           for (i in button_ids) {
             if (button_ids[i] == button_id) alreadyHasId = true;
           }
            if (!alreadyHasId) button_ids.push(button_id);
          }
          getButtonUuids(user_uuid, button_ids);
        }
      });
    }
  });
}

function getButtonUuids(user_uuid, button_ids) {
  pg.connect(pgWebUrl, function(err, pgWebClient) {
    if (err != null) {
    	console.log("Could not connect to web postgres: " + err);
    	callback(err);
    } else {
      pgWebClient.query("SELECT uuid FROM buttons WHERE id IN (" + button_ids.join() + ")", function(err, result) {
        if (err != null) {
          console.log("finding button uuids unsuccessful: " + err);
        } else {
          button_uuids = [];
          for (row in result.rows) {
            button_uuids.push(result.rows[row].uuid);
          }
          resetCounts(user_uuid, button_uuids);
        }
      });
    }
  });
}

function resetCounts(user_uuid, button_uuids) {
  redisDataClient.set('user:' + user_uuid, '0', function(err, result) {
    if (err != null) {
      console.log("setting redis user click count error: " + err);
    }
  });
  
  for (i in button_uuids) {
    redisDataClient.set(user_uuid + ':' + button_uuids[i], '0', function(err, result) {
      if (err != null) {
        console.log("setting redis user click count error: " + err);
      } else {
        console.log("user successfully reset!");
        process.exit(code=0);
      }
    });
  }
}

prompt.start();
prompt.get(['email'], function(err, result) {
  if (err != null) {
    console.log("prompt error - couldn't get user email");
  } else {
    console.log("Resetting User " + result.email);
    resetUser(result.email);
  }
});




