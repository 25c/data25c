module.exports = {
  development: {
    postgresql: {
      host: "localhost",
      database: "data25c_development",
      user: "superuser"
    }
  },
  test: {
    postgresql: {
      host: "localhost",
      database: "data25c_test",
      user: "superuser",
      password: ""
    }
  }
};

if (process.env.DATABASE_URL) {
	var url = require('url').parse(process.env.DATABASE_URL);
	module.exports.production = {
		postgresql: {
			host: url.hostname,
			database: url.pathname.substring(1),
			user: url.auth.split(":")[0],
			password: url.auth.split(":")[1]
		}
	}
}
