// Configuration server address

exports.configserver = 'http://127.0.0.1:8075';


// Logging

exports.log = {

	enabled: true,
	byPrefix: {
		'db': false,
		'orm': false
	}

};

// Database configuration

exports.db = {

	// Database instances

	instances: {
		'default': {
//			host: '10.134.165.234',
			host: '144.64.229.164',
//			host: '127.0.0.1',
			port: 27017,
			name: 'codebits',
			opts: { server: {auto_reconnect: true} }
		}
	}

};
