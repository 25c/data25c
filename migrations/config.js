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
  },
	production: {
		postgresql: {
			host: "ec2-23-23-234-187.compute-1.amazonaws.com",
			port: 5432,
			user: "zwudoojwkmzxws",
			database: "d4j8bu1un1lrsv",
			password: "Wuzf1d-95lwV5zXCKJuwB8VheN"
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
