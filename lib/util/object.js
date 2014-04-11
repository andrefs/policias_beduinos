"use strict";

var
	crypto = require('crypto'),
	ObjectID = require('mongodb').ObjectID;


// Clone an object

exports.clone = function(obj) {
	if(obj == null || typeof(obj) != 'object')
		return obj;

	// Regexp

	if ( obj instanceof RegExp )
		return new RegExp(obj.source,obj.toString().replace(/[\s\S]+\//, ""));
	else if ( obj instanceof ObjectID )
		return new ObjectID(obj.toString());

	// Normal object

	var
		temp = obj.constructor();

	for(var key in obj) {
		try {
			temp[key] = this.clone(obj[key]);
		}
		catch(ex) {
			console.log("Fuckup ("+ex.toString()+") cloning object key "+key+". Object dump:");
			console.log(obj);
			temp[key] = null;
		}
	}

	return temp;
}

// Clone just the first level of an object

exports.simpleclone = function(obj) {
	if(obj == null || typeof(obj) != 'object')
		return obj;

	var
		temp = obj.constructor();

	for(var key in obj)
		temp[key] = obj[key];

	return temp;
};

// Merge objects

exports.merge = function(dest,source,overwrite,overwriteObjs) {

	var
		self = this,
		objs = (source instanceof Array) ? source : [source];

	objs.forEach(function(o){
		for ( var p in o ) {
			if ( dest[p] != null && !overwriteObjs ) {
				if ( dest[p] instanceof Array && o[p] instanceof Array ) {
					if ( overwrite )
						dest[p] = o[p];
					else {
						o[p].forEach(function(v){
							dest[p].push(v);
						});
					}
				}
				else if ( typeof(dest[p]) == "object" && typeof(o[p]) == "object" ) {
					self.merge(dest[p],[o[p]],overwrite);
				}
				else {
					if ( overwrite )
						dest[p] = o[p];
					// else { leave like that }
				}
			}
			else
				dest[p] = o[p];
		}
	});

	return dest;

};

/*
  Create a hash value of an object
 */

exports.hash = function(o,format) {

	var
		ret = { data: "" };

	if ( !format )
		format = "hex";

	// Array or string ?

	if ( o instanceof Array )
		_hashArray(o,ret);
	else if ( typeof(o) == "object" )
		_hashObject(o,ret);
	else
		ret.data = o.toString();

	// Format is string ?

	if ( format == "string" )
		return ret.data;

	// Build hash

	var hash = crypto.createHash('md5').update(ret.data).digest((format == "int")?"binary":format);

	return (format == "int") ? _hashToInt(hash) : hash;

};
function _hashToInt(h) {

	// Return integer hash

	return parseInt(
		 h.charCodeAt(0) +
		(h.charCodeAt(1)  * 256) +
		(h.charCodeAt(2)  * Math.pow(256,2)) +
		(h.charCodeAt(3)  * Math.pow(256,3)) +
		(h.charCodeAt(15) * Math.pow(256,4))
	);

};
function _hashObject(o,ret) {

	var
		keys = Object.keys(o).sort(),
		k;

	// Compile data

	ret.data += "{#o}";
	keys.forEach(function(k){
		if ( k.charAt(0) == "_" )
			return;
		ret.data += "{#V "+k+"} ";
		if ( typeof(o[k]) == "object" ) {
			if ( o[k] instanceof Date )
				ret.data += "{#S "+k+"} " + o[k].toString() + " {/#S} ";
			else if ( o[k] instanceof Array )
				_hashArray(o[k],ret);
			else if ( o[k] == null ) {
				ret.data += "(NULL)";
			}
			else {
				try {
					_hashObject(o[k],ret);
				}
				catch(ex){
					console.log("Error hashing object on property '"+k+"' and value '"+o[k].toString()+"': ",ex);
				}
			}
		}
		else if ( o[k] == null )
			ret.data += "{#S "+k+"} _"+String.fromCharCode(1)+"_null_"+String.fromCharCode(1)+"_ {/#S} ";
		else
			ret.data += "{#S "+k+"} " + o[k].toString() + " {/#S} ";
		ret.data += " {/#V} ";
	});
	ret.data += " {/#o} ";
	return ret;

};
function _hashArray(o,ret) {

	var
		data = "";

	// Compile data

	ret.data += "{#a} ";
	o.forEach(function(v){
		if ( typeof(v) == "object" )
			if ( v instanceof Array )
				_hashArray(v,ret);
			else
				_hashObject(v,ret);
		else
			ret.data += "{#s} " + v.toString() + " {/#s} ";

	});
	ret.data += " {/#a} ";
	return ret;

};


// Get property value by name
exports.getPropertyValue = function(o,p) {

	var
		parts = p.split("."),
		part;

	while ( parts.length && (part = parts.shift()) != null ) {
		o = o[part];
		if ( o == null || (parts.length && typeof(o) != "object") )
			return null;
	}

	return o;

};

// Get all property values
exports.getAllPropertyValues = function(o,p) {

	var
		values = [];

	this.iteratePropertyValues(o,p,function(v){
		values.push(v);
	});

	return values;

}


// Iterate over property values
exports.iteratePropertyValues = function(o,p,handler) {

	return _iteratePropertyValues(o,p,handler);

}
function _iteratePropertyValues(o,p,handler) {

	var
		parts = p.split("."),
		part,
		parent;

	// We have property components, we have to follow them
	while ( parts.length && (part = parts.shift()) != null ) {
		parent = o;
		o = o[part];
		if ( o instanceof Array ) {
			for ( var idx = 0 ; idx < o.length ; idx++ )
				_iterateArrayVal(o,idx,o[idx],parts.join("."),handler);
			continue;
		}
		if ( o == null || (parts.length && typeof(o) != "object") )
			return;
		if ( parts.length == 0 ) {
			var rv = handler(o);
			if ( rv != null )
				parent[part] = rv;
		}
	}

}
function _iterateArrayVal(o,idx,val,p,handler) {

	if ( p == "" ) {
		var rv = handler(val);
		if ( rv != null )
			o[idx] = rv;
	}
	else
		return _iteratePropertyValues(val,p,handler);

}


// Set property value by property name

exports.setPropertyValue = function(o,p,v) {

	var
		parts = p.split("."),
		part;

	while ( parts.length > 1 ) {
		part = parts.shift();
		if ( o[part] == null || typeof(o[part]) != "object" )
			o[part] = {};
		o = o[part];
	}
	o[parts.shift()] = v;

};


// Delete a property from an object (with the change of deleting parents if they'r empty)

exports.deleteProperty = function(o,p,hierarchy) {

	var
		pparts = (typeof(p) == "string") ? p.split('.') : p,
		oprop,
		oparts = [ o ];

	// Go to the last parent
	// If find one element that is an array, delete properties from every element

	oprop = o;
	for ( var x = 0 ; x < pparts.length-1 ; x++ ) {
		var part = pparts[x];
		oprop = oprop[part];
		if ( oprop == null )
			break;
		if ( typeof(oprop) != "object" )
			return false;
		if ( oprop instanceof Array ) {
			var hparts = [];
			for ( var y = x+1 ; y < pparts.length ; y++ )
				hparts.push(pparts[y]);
			for ( var y = 0 ; y < oprop.length ; y++ ) {
				deleteProperty(oprop[y],hparts,hierarchy);
				if ( hierarchy && isEmpty(oprop[y]) )
					oprop.splice(y,1);
			}
		}
		oparts.push(oprop);
	}

	// Delete property

	if ( oprop )
		delete oprop[pparts.pop()];

	// Hierarchy

	if ( hierarchy ) {
		for ( var x = oparts.length - 1 ; x > 0 ; x-- ) {
			if ( oparts[x] instanceof Array && oparts[x].length == 0 && x > 0 )
				delete oparts[x-1][pparts[x-1]];
			else if ( isEmpty(oparts[x]) && x > 0 )
				delete oparts[x-1][pparts[x-1]];
		}
	}

	return true;

};


// Return an array with the values of a specific property on a list of objects

exports.valueList = function(objs,prop) {

	var
		r = [];

	objs.forEach(function(o){
		r.push(exports.getPropertyValue(o,prop));
	});

	return r;

};


// Group objects by their values on a property

exports.group = function(objs,prop) {

	var
		groups = {};

	objs.forEach(function(o){
		var v = exports.getPropertyValue(o,prop);
		if ( groups[v] == null )
			groups[v] = [];
		groups[v].push(o);
	});

	return groups;

};


// Map an object to other

exports.map = function(map,source,target) {

	var
		self = this,
		o = target ? target : {};

	for ( var p in map ) {
		var v = self.getPropertyValue(source,map[p]);
		if ( v != null )
			self.setPropertyValue(o,p,v);
	}

	return o;

};

// Grep an array

exports.grep = function(arr,verify) {

	var
		func = (verify instanceof RegExp) ? function(v){return v.match(verify);} : func,
		ret = [];

	if ( arr == null )
		return [];
	if ( arr instanceof Array && typeof func == "function" ) {
		arr.forEach(function(val){
			if ( func(val) )
				ret.push(val);
		});
	}
	else if ( typeof arr == "object" && verify instanceof Array ) {
		for ( var k in arr ) {
			if ( k == verify )
				ret.push(arr[k]);
		}
	}
	else if ( typeof arr == "object" && typeof verify == "function" ) {
		for ( var k in arr ) {
			if ( verify(arr[k]) )
				ret.push(arr[k]);
		}		
	}

	return ret;
};
