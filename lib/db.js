var
	log	= require('./log').logger("db"),
	conf	= require('./conf').get("db"),

	util	= require('util'),
	events	= require('events'),
	async	= require('async'),
	mongodb	= require('mongodb'),
	object	= require('./util/object'),

	_instanceCache = { },
	_waitingDBCallbacks = [];


// Connect to database
// Syntax:
//    .open(callBack)

var open = function(handler) {

	var
		self = this,
		opts = { server: {}, db: {} };

	// Validation

	if ( !self || !self._conf )
		return handler ? handler(new Error("No configuration")) : null;
	if ( self._conf.host == null || self._conf.port == null || self._conf.name == null )
		return handler ? handler(new Error("Instance host/port/name not configured")) : null;

	// Options

	if ( self._conf.opts ) {
		opts.server	= self._conf.opts.server	|| {};
		opts.db		= self._conf.opts.db		|| {};
	}

	// Defaults to writable

	if ( opts.db && opts.db.w == null )
		opts.db.w = (self._conf.readOnly) ? -1 : 1;


	log.INFO(self._instance+": Openning database connection to "+self._conf.name+"@"+self._conf.host+":"+(self._conf.port||27017)+"...");

	// Connect

	var
		client = new mongodb.Db(self._conf.name, new mongodb.Server(self._conf.host, self._conf.port || 27017, opts.server), opts.db);

	// Open

	self._connecting = true;
	self._client = null;
	self._db = null;
	self._started = new Date();
	client.open(function(err,db){
		self._connecting = false;
		if ( !err ) {
			log.INFO(self._instance+": Connected! (after "+(new Date()-self._started)+" ms)");
			self._client = client;
			self._db = db;
		}
		else
			log.ERROR(self._instance+": Error connecting (after "+(new Date()-self._started)+" ms): ",err);

		// Notify who requested a database

		while ( _waitingDBCallbacks.length ) {
			var cb = _waitingDBCallbacks.shift();
			cb(err,db);
		}
		return handler(err,self);
	});

	return self;

};


// Disconnect
// Syntax:
//	.close([force,][handler]);

var close = function(force,handler) {

	var
		self = this;

	log.info(self._instance+": Closing database connection");

	self._db.close(force,function(err){
		log.info(self._instance+": Closed database connection.");
		self._db = null;
		if ( handler )
			return handler(err);
	});

	return self;

};


// Check if database connection is open

var check = function(handler) {

	var
		self = this;

	if ( self._db )
		return handler(self._db);

	_waitingDBCallbacks.push(handler);
	if ( !self._connecting )
		return self.open(function(){ });

};


// Get a collection
// Syntax:   collection(name,handler);
// Callback: function(err,col)

var collection = function(name,handler) {

	var
		self = this;

	// Is the connection openned ?

	if ( self._db == null ) {
		return self.check(function(err){
			if ( err )
				return handler({ where: "core.db", type: "hard", code: "EDBCONCK", description: err.toString() },null);

			return self._db.collection(name,handler);
		});
	}

	self._db.collection(name,handler);

	return self;

};


// Find a document and return it (or null, if was not found)
// Syntax:   findOne(collection[,query[,fields]],callBack)
// Callback: function(err,doc)

var findOne = function(collection,query,fields,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,4);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	fields		= args.shift()	|| {};

	// Find

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Finding a document on "+collection+" matching query "+JSON.stringify(query));
		col.findOne(query,fields,handler);
	});

	return self;

};


// Find and return an array with the results
// Syntax:   find(collection[,query[,fields[,opts]]],callBack)
// Callback: function(err,docs)

var find = function(collection,query,fields,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5),
		rows = [];

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	fields		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	// Query has $or ? Use findMulti
	if ( query['$or'] )
		return self.findMulti(collection,query,fields,opts,handler);

	// Find
	self.findCursor(collection,query,fields,opts,function(err,cursor){
		if ( err )
			return handler(err,null);

		cursor.each(function(err,row){
			if ( err ) {
				log.error("Cursor error: ",err);
				return handler({where: "core.db", type: "hard", code: "ECURSERR", description: err.toString()},null);
			}
			if ( row )
				return rows.push(row);

			return handler(null,rows);
		});

	});

	return self;

};


// Find, running the $or queries in paralell, join the results, sort, slice them and return the results
// Syntax:   findMulti(collection[,query[,fields[,opts]]],callBack)
// Callback: function(err,cursor)

var findMulti = function(collection,query,fields,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5),
		origLimit;

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	fields		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	// Query has no $or ?
	if ( query['$or'] == null )
		return self.find(collection,query,fields,opts,handler);

	// Has limit and skip ? Increment skip to limit (we will skip on client-side)
	if ( !opts.skip )
		opts.skip = 0;
	origLimit = opts.limit;
	if ( opts.limit != null && opts.skip > 0 )
		opts.limit += opts.skip;

	// If have field specification, garantee that sort fields are there
	// Later we need to remove them
	if ( opts.fields != null && Objects.keys(opts.fields).length == 0 ) {
		var haveExcluding = false;
		for ( var f in opts.fields ) {
			if ( opts.fields[f] == false ) {
				haveExcluding = true;
				break;
			}
		}
		if ( !haveExcluding ) {
			opts.sort.forEach(function(sort){
				var
					f = sort[0];
				opts.fields[f] = true;
			});
		}
	}

	// Work conditions:
	//   If condition have something outside or $or, put it inside every $or
	for ( var expr in query ) {
		if ( expr != "$or" ) {
			query['$or'].forEach(function(e){
				e[expr] = query[expr];
			});
		}
	}

	// Get collection and find() in parallel

	this.collection(collection, function(err, col) {
		if ( err )
			return handler(err,null);

		// Build queries and options

		async.map(query['$or'],function(q,next){
			col.find(q,fields,opts||{},function(err,cursor){
				if ( err )
					return next(err,null);
				var
					rows = [];

				cursor.each(function(err,row){
					if ( err ) {
						log.error("Cursor error: ",err);
						return next({where: "core.db", type: "hard", code: "ECURSERR", description: err.description},null);
					}
					if ( row )
						return rows.push(row);

					return next(null,rows);
				});
			});
		},function(err,results){
			if ( err )
				return handler(err,null);

			// Merge and sort

			var allResults = _findMultiSort(_findMultiMerge(results),opts.sort);

			// Cut

			if ( opts.limit || opts.skip )
				allResults = _findMultiCut(allResults,opts.skip,origLimit ? origLimit : allResults.length-opts.skip);

			return handler(null,allResults);

		});

	});

	return self;

};

// Merge all data results, avoiding dupplicates

var _findMultiMerge = function(pieces) {

	var
		all = [ ],
		_ids = { };

	pieces.forEach(function(p){
		if ( !(p instanceof Array) )
			return;
		p.forEach(function(i){
			if ( i._id && _ids[i._id.toString()] == null ) {
				all.push(i);
				_ids[i._id.toString()] = true;
			}
		});
	});
	return all;

};

// Sort all the results according to the rules

var _findMultiSort = function(data,rules) {

	// No rules? Err..

	if ( rules == null || typeof(rules) != "object" )
		return data;

	// If sort rules are not an Array, convert it

	if ( !(rules instanceof Array) ) {
		rules = [];
		for ( var p in rules )
			rules.push([f,rules[p]]);
	}

	// Sort based on properties

	return data.sort(function(a,b){
		for ( var x = 0 ; x < rules.length ; x++ ) {
			var
				p = rules[x][0],
				ori = rules[x][1],
				vA = object.getPropertyValue(a,p),
				vB = object.getPropertyValue(b,p);

			if ( vA < vB )
				return 0 - ori;
			else if ( vA > vB )
				return 0 + ori;
		}
		return 0;
	});

}

// Cut the results

function _findMultiCut(data,start,limit) {

	return data.splice(start || 0,limit);

}


// Find and return a cursor
// Syntax:   findCursor(collection[,query[,fields[,opts]]],callBack)
// Callback: function(err,cursor)

var findCursor = function(collection,query,fields,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	fields		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	// Find
	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Finding documents on "+collection+" matching query "+JSON.stringify(query)+" (fields: "+JSON.stringify(fields)+", opts: "+JSON.stringify(opts)+")");
		return col.find(query,fields,opts||{},handler);
	});

	return self;

};


// Find and locally filter results

var findGrep = function(collection,query,fields,opts,grepFn,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,6),
		rows = [];

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	grepFn		= args.pop()	|| function(){return true};
	query		= args.shift()	|| {};
	fields		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	self.findCursor(collection,query,fields,opts,function(err,cursor){
		if ( err )
			return handler(err,null);

		cursor.each(function(err,row){
			if ( err ) {
				log.error("Cursor error: ",err);
				return handler({where: "core.db", type: "hard", code: "ECURSERR", description: err.toString()},null);
			}
			if ( row ) {
				if ( grepFn(row) )
					rows.push(row);
				return;
			}
			return handler(null,rows);
		});	
	});

	return self;

};


// Count all the records on a collection (or matching a specific query)
// Syntax:   count(collection[,query],callBack)
// Callback: function(err,numdocs)

var count = function(collection,query,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,4);

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	// Count

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Counting documents on "+collection+" matching query "+JSON.stringify(query));
		return col.count(query,opts,handler);
	});

	return self;

};


// Get the distinct fields values of a collection (optionally for results matching a specific query)
// Syntax:   distinct(collection,field,query,callBack)
// Callback: function(err,values)

var distinct = function(collection,field,query,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,4);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	field		= args.shift()	|| _error("No field");
	query		= args.shift()	|| {};

	// Get distinct values

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		return col.distinct(field,query,handler);
	});

	return self;

};


// Insert documents on the database
// Syntax:   insert(collection,documents[,opts],callback)
// Callback: function(err,docs)

var insert = function(collection,docs,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	docs		= args.shift()	|| _error("No documents");
	opts		= args.shift()	|| {};

	if ( !(docs instanceof Array) )
		docs = [docs];

	// Insert
	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Insertin "+docs.length+" documents on "+collection+" (opts: "+JSON.stringify(opts)+")");
		return col.insert(docs,opts,handler);
	});

	return self;

};


// Update documents
// Syntax:   update(collection,query,update[,opts],callback)
// Callback: function(err,docs)

var update = function(collection,query,update,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| _error("No query");
	update		= args.shift()	|| _error("No update operation");
	opts		= args.shift()	|| {};

	// Get collection

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		// Update

		log.info(self._instance+": Updating documents on "+collection+" matching query "+JSON.stringify(query)+" with operation: "+JSON.stringify(update)+" (opts: "+JSON.stringify(opts)+")");
		return col.update(query,update,opts,handler);
	});

	return self;

};


// Perform a series of updates
// Syntax:   updateSeries(collection,queriesUpdates,opts,handler);
// Callback: function(err,results)

var updateSeries = function(collection,queriesUpdates,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	queriesUpdates	= args.shift()	|| _error("No query");
	opts		= args.shift()	|| {};

	// Get collection

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		// Update in series

		log.info(self._instance+": Performing "+queriesUpdates.length+" updates in series on "+collection+"...");
		async.mapSeries(queriesUpdates,function(queryUpdate,next){
			col.update(queryUpdate[0],queryUpdate[1],opts,function(err,res){
				if ( err )
					return next(err,null);

				return next(null,res);
			});
		},handler);
	});

	return self;

};


// Remove
// Syntax:   remove(collection[,query[,opts]],callback)
// Callback: function(err,numdocs)

var remove = function(collection,query,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,5);

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	collection	= args.shift()	|| _error("No collection");
	query		= args.shift()	|| {};
	opts		= args.shift()	|| {};

	// Update

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Removing documents on "+collection+" matching query "+JSON.stringify(query));
		return col.remove(query,opts,handler);
	});

	return self;

};


// Drop
// Syntax:   drop(collection,callback)
// Callback: function(err,reply)

var drop = function(collection,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,2);

	// Check arguments

	collection	= args.shift()	|| _error("No collection");
	handler		= args.pop()	|| _error("No callback");

	// Get input collection

	self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Dropping collection '"+collection+"'");
		return col.drop(handler);
	});

	return self;

};


// Rename
// Syntax:   rename(collection,newCollection,opts,callback)
// Callback: function(err,ok)

var rename = function(collection,newCollection,opts,handler) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,4);

	// Check arguments

	collection	= args.shift()	|| _error("No collection");
	newCollection	= args.shift()	|| _error("No new collection name");
	handler		= args.pop()	|| _error("No callback");
	opts		= args.pop()	|| {};

	// Get input collection

	return self.collection(collection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Renaming collection '"+collection+"' to '"+newCollection+"'");
		return col.rename(newCollection,opts,handler);
	});

};


// MapReduce
// Syntax:   mapReduce(inCollection,outCollection,mapFn,reduceFn[,opts],callback)
// Callback: function(err,newCollection)

var mapReduce = function(inCollection,out,mapFn,reduceFn,opts,callback) {

	var
		self = this,
		args = Array.prototype.slice.call(arguments,0,6),
		opts = { out: ((typeof out == "string") ? {replace: out} : out) };

	// Check arguments

	handler		= args.pop()	|| _error("No callback");
	inCollection	= args.shift()	|| _error("No input collection");
	outCollection	= args.shift()	|| _error("No output collection");
	mapFn		= args.shift()	|| _error("No map function");
	reduceFn	= args.shift()	|| _error("No reduce function");
	uopts		= args.shift()	|| {};

	opts = object.merge(opts,uopts);

	// Get input collection

	return self.collection(inCollection,function(err,col){
		if ( err )
			return handler(err,null);

		log.info(self._instance+": Performing a mapReduce on '"+inCollection+"' collection...");
		return col.mapReduce(mapFn,reduceFn,opts,handler);
	});

};


// Find the best index for a query and sort based on conf.indexes
// Syntax:	findBestIndex(collection,query,sort)
// Returns:	index name or null

var findBestIndex = function(collection,query,sort) {

	var
		self = this,
		fixedValue = {},
		rangeValues = {},
		sortFields = {},
		totalProps = 0,
		indexes = [],
		startDate = new Date(),
		rangeFields = ((conf.arrayFields && typeof conf.arrayFields == "object") ? conf.arrayFields[collection] : null) || {};

	if ( !conf.indexes || typeof conf.indexes != "object" )
		return;

	indexes = conf.indexes[collection] || [];
	if ( indexes.length == 0 )
		return;

	// Get fixed and range value properties
	for ( var p in query ) {
		totalProps++;
		if ( typeof(query[p]) != "object" && !rangeFields[p] )
			fixedValue[p] = query[p];
		else {
			// If query contains a $where, is better not to touch
			if ( query[p]['$where'] )
				return;
			rangeValues[p] = query[p];
		}
	}

	// Get sort fields
	if ( sort ) {
		if ( sort instanceof Array ) {
			sort.forEach(function(rule){
				totalProps++;
				if ( rule instanceof Array )
					sortFields[rule[0]] = rule[1];
				else if ( typeof(rule) == "object" ) {
					for ( var f in rule ) {
						if ( rule[f] == 1 || rule[f] == -1 )
						sortFields[f] = rule[f];
						break;
					}
				}
			});
		}
		else if ( typeof(sort) == "object" ) {
			for ( var f in sort ) {
				if ( sort[f] == 1 || sort[f] == -1 ) {
					totalProps++;
					sortFields[f] = sort[f];
				}
			}
		}
	}
	if ( totalProps == 0 )
		return;

	// Find a nice index
	var
		idx = _findBestIndex(indexes,fixedValue,rangeValues,sortFields,totalProps);

	if ( idx != null ) {
		log.info(self._instance+": Assigning index "+JSON.stringify(idx)+" to the query: ",JSON.stringify({query: query, sort: sort}));
		return idx;
	}

};

var _findBestIndex = function(indexes,fixedValue,rangeValues,sortFields,totalProps) {

	var
		idx,
		choosenIdx = null,
		bestLostProps = 9999;

	// Run all indexes
	indexes: for ( var x = 0 ; x < indexes.length ; x++ ) {
		idx = indexes[x];

		// Find the position of each property
		var
			idxPropPos = {},
			ppos = 0,
			lostProps = 0,
			firstRange = null,
			lastFixed = null,
			lastIdxMatch = null;

		// Mark all index property positions
		// Check if index starts with fixed properties
		for ( var p in idx ) {
			if ( fixedValue[p] != null ) {
				if ( ppos == 0 )
					lastIdxMatch = 0;
				else if ( ppos > 0 && lastIdxMatch != null )
					lastIdxMatch = ppos;
			}
			else if ( ppos == 0 )
				continue indexes;
			idxPropPos[p] = ppos++;
		}

		// Find first position of a range value property
		for ( var p in rangeValues ) {
			if ( idxPropPos[p] == null )
				lostProps++;
			else if ( firstRange == null || firstRange > idxPropPos[p] )
				firstRange = idxPropPos[p];
		}

		// Find last position of a fixed value property
		for ( var p in fixedValue ) {
			if ( idxPropPos[p] == null )
				lostProps++;
			else if ( lastFixed == null || lastFixed < idxPropPos[p] )
				lastFixed = idxPropPos[p];
		}
		if ( lastFixed == null || firstRange == null )
			continue indexes;

		// Find the sort fields
		for ( var p in sortFields ) {
			if ( idxPropPos[p] == null )
				lostProps++;
			else if ( idxPropPos[p] < lastFixed )
				continue indexes;
			else if ( idxPropPos[p] > firstRange )
				continue indexes;
		}

		if ( lostProps > 0 || lastFixed > firstRange )
			continue indexes;
		if ( lostProps < bestLostProps )
			choosenIdx = idx;
	}

	return choosenIdx;

};


// Get a database instance

var instance = function(name,_conf) {

	if ( !_conf && (conf.instances == null || conf.instances[name] == null) ) {
		throw new Error("Database instance does not exist");
		return null;
	}

	if ( _instanceCache[name] )
		return _instanceCache[name];

	var i = {
		_client:	null,
		_db:		null,
		_connecting:	false,
		_instance:	name,
		_conf:		(_conf || conf.instances[name]),

		// Methods

		open:		open,
		connect:	open,
		close:		close,
		disconnect:	close,
		check:		check,
		collection:	collection,
		findOne:	findOne,
		find:		find,
		findMulti:	findMulti,
		findCursor:	findCursor,
		findGrep:	findGrep,
		count:		count,
		distinct:	distinct,
		insert:		insert,
		update:		update,
		updateSeries:	updateSeries,
		remove:		remove,
		drop:		drop,
		rename:		rename,
		mapReduce:	mapReduce,
		findBestIndex:	findBestIndex
	};
	util.inherits(i, events.EventEmitter);
	_instanceCache[name] = i;

	return i;

};


// Util functions

var _error = function(msg){

	throw new Error(msg);

};

// Exported functions

exports.ObjectID	= function(id) {

	if ( typeof(id) == "string" )
		return new mongodb.ObjectID(id);
	return id;

};

exports.instance	= instance;
exports.db		= instance;
