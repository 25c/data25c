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
	staging: {
		postgresql: {
			host: "ec2-107-21-108-69.compute-1.amazonaws.com",
			port: 5432,
			user: "tlylvakprmhpdq",
			database: "ddkgocuj17dmq",
			password: ""
		}
	},
	production: {
		postgresql: {
			host: "ec2-23-23-235-211.compute-1.amazonaws.com",
			port: 5782,
			user: "u41ipq777smjl8",
			database: "dc5v5e02h2gkp1",
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
