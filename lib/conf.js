"use strict";

var
	object		= require('./util/object'),
	lconf		= require('../conf/core'),

	confCache	= { },
	confUpdates	= { };


// GET
exports.get = function(path) {

	return object.getPropertyValue(lconf,path);

};
