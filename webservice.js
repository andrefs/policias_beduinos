#!/usr/bin/env node

var
	fs = require('fs'),
	http = require('http'),
	qs = require('querystring'),
	db = require('./lib/db'),
	totals,
	totalKeys  = 0,
	totalOcurs = 0,
	cache = {};


// Read totals JSON

console.log("Reading totals..");
totals = JSON.parse(fs.readFileSync("totals.json").toString());
totalKeys = Object.keys(totals).length;
for ( var k in totals )
	totalOcurs += totals[k];
console.log("totals: "+totalKeys+" / "+totalOcurs);


// Webçerber

http.createServer(function (req, res) {

	req.originalURL = req.url;
	req.reachTime = new Date();
	if ( req.url && req.url.match(/\?(.*)$/) ) {
		var args = RegExp.$1;
		req.args = qs.parse(args);
		req.url = req.url.replace(/\?.*$/,"");
	}
	if ( !req.args )
		req.args = {};

	if ( getCache(req,res) )
		return; 

	if ( req.url == "/wordsByDate" ) {
		if ( !req.args.start || !req.args.end ) {
			res.writeHead(500, {'Content-Type': 'text/plain'});
			res.end('Manda start e end carago!');
			return;
		}
		req.args.start = parseInt(req.args.start) * 1000000;
		req.args.end = parseInt(req.args.end) * 1000000 + 1;

		getWordsByDate(req.args.start,req.args.end,function(err,data){
			if ( err ) {
				res.writeHead(500, {'Content-Type': 'text/plain'});
				res.end('A query deu erros: ',JSON.stringify(err));
				return;
			}
			res.writeHead(200, {'Content-Type': 'text/plain; charset=utf-8'});
			answer(req,res,JSON.stringify(data));
		});

	}
	else if ( req.url == "/relevantWordsByDate" ) {
		if ( !req.args.start || !req.args.end ) {
			res.writeHead(500, {'Content-Type': 'text/plain'});
			res.end('Manda start e end carago!');
			return;
		}
		req.args.start = parseInt(req.args.start) * 1000000;
		req.args.end = parseInt(req.args.end) * 1000000 + 1;
		req.args.occurs = parseInt(req.args.occurs) || 10;

		getRelevantWordsByDate(req.args.start,req.args.end,req.args.occurs,function(err,relevants){
			if ( err ) {
				res.writeHead(500, {'Content-Type': 'text/plain'});
				res.end('A query deu erros: ',JSON.stringify(err));
				return;
			}

			res.writeHead(200, {'Content-Type': 'text/plain; charset=utf-8'});
			answer(req,res,JSON.stringify(relevants));
		});
	}
	else if ( req.url == "/getTopics" ) {

		if ( !req.args.start || !req.args.end ) {
			res.writeHead(500, {'Content-Type': 'text/plain'});
			res.end('Manda start e end carago!');
			return;
		}
		req.args.start		= parseInt(req.args.start) * 1000000;
		req.args.end		= parseInt(req.args.end) * 1000000 + 1;
		req.args.occurs		= parseInt(req.args.occurs)		|| 10;
		req.args.threshold	= parseInt(req.args.threshold)		|| 10;
		req.args.cluster	= (req.args.cluster && req.args.cluster != "false") ? true : false;
		req.args.contents	= parseInt(req.args.contents)		|| 0;
		return getTopics(req.args.start,req.args.end,req.args.occurs,req.args.threshold,req.args.cluster,function(err,topics){
			if ( err ) {
				res.writeHead(500, {'Content-Type': 'text/plain'});
				res.end('Erro a obter os topicos');
				return;
			}

			// No contents?
			if ( !req.args.contents )
				return answer(req,res,200,JSON.stringify(topics));

			// Hash content ids
			var ids = {};
			topics.forEach(function(topic){
				if ( !topic.ids )
					return;
				var c = -1;
				topic.ids.forEach(function(id){
					c++;
					if ( c >= req.args.contents )
						return;
					if ( !ids[id] )
						ids[id] = [];
					ids[id].push(topic);
				});
			});
			if ( Object.keys(ids).length == 0 )
				return answer(req,res,200,JSON.stringify(topics));

			// Get contents
			return getContents(Object.keys(ids),function(err,contents){
				if ( err ) {
					console.log("Error getting contents: ",err);
					return answer(req,res,200,JSON.stringify(topics));
				}

				// Find topics for each content
				contents.forEach(function(c){
					if ( !ids[c.id] )
						return;
					ids[c.id].forEach(function(topic){
						if ( !topic.contents )
							topic.contents = [];
						topic.contents.push(c);
					});
				});

				return answer(req,res,200,JSON.stringify(topics));
			});

		});

	}
	else {
		res.writeHead(404, {'Content-Type': 'text/plain'});
		res.end('N percebes nada disto');
		return;
	}

}).listen(8080, '0.0.0.0');
console.log('Server running at http://0.0.0.0:8080/');



// Functions

function answer(req,res,status,data) {

	var
		k = req.cache_key || req.url;

//	console.log("SET: "+k);
	cache[k] = { status: status, data: data, expires: new Date().getTime() + 12000000 };
	res.writeHead(cache[k].status, {'Content-Type': 'text/plain; charset=utf-8'});
	res.end(cache[k].data);

	console.log(req.originalURL+" "+(new Date()-req.reachTime)+" ms (L)");
}

function getCache(req,res) {

	var
		k = req.url + "?";

	Object.keys(req.args).sort().forEach(function(arg){
		k += arg+"="+req.args[arg]+"&";
	});
	req.cache_key = k;
//	console.log("GET: "+k);

	if ( cache[k] ) {
		if ( cache[k].expires > new Date().getTime() ) {
			res.writeHead(cache[k].status, {'Content-Type': 'text/plain; charset=utf-8'});
			res.end(cache[k].data);
			console.log(req.originalURL+" "+(new Date()-req.reachTime)+" ms (C)");
			return true;
		}
		else {
			delete cache[k];
		}
	}
	return false;

}

function getWordsByDate(start,end,handler) {

	var qstart = new Date();
	return db.instance("default").find("wordsByDate",{date:{$gt:start,$lt:end},imp:1,word:{$ne:"_orig"}},{word:1,ocurs:1,id:1},function(err,data){
		if ( err ) {
			console.log("A query deu errors: ",err);
			return handler(err,null);
		}
		console.log("Got words by date in "+(new Date()-qstart)+" ms");

		var
			byWord = {},
			retList = [],
			grouped = 0;

		data.forEach(function(d){
			if ( d.word == "_orig" )
				return;
			if ( byWord[d.word] ) {
				byWord[d.word].ocurs += d.ocurs;
				if ( !byWord[d.word].grouped )
					byWord[d.word].grouped = 1;
				else
					byWord[d.word].grouped++;
				if ( !byWord[d.word].ids ) {
					byWord[d.word].ids = { };
					byWord[d.word].ids[d.id] = true;
				}
				else
					byWord[d.word].ids[d.id] = true;
				delete d['id'];

				grouped++;
				return;
			}
			byWord[d.word] = d;
			d.p = totals[d.word] / totalKeys;		// Alterar esta regra para outra mais apropriada. Ao gosto do freguês
			retList.push(d);
			delete d['_id'];
		});

		// Convert ids to Array
		retList.forEach(function(i){
			if ( i.ids )
				i.ids = Object.keys(i.ids);
		});
		retList = retList.sort(function(a,b){
			return (a.p > b.p) ? 1 : (a.p < b.p) ? -1 : 0;
		});

		console.log("Returned list of word for the period "+start+" -> "+end+" with "+grouped+" grouped results");
		return handler(null,retList);
	});

};

function getRelevantWordsByDate(start,end,min_occurs,handler) {

	return getWordsByDate(start,end,function(err,retList){
		var relevants = [];
		retList.forEach(function(item){
			if(item.ocurs < min_occurs)
				return;
			if ( !totals[item.word] )
				return;
			item.p = totals[item.word] / totalKeys;		// Alterar esta regra para outra mais apropriada. Ao gosto do freguês
//			item.p = totals[item.word] / totalOcurs;	// Alterar esta regra para outra mais apropriada. Ao gosto do freguês
			if ( item.p < 0.001 )
				relevants.push(item);
		});
		relevants = relevants.sort(function(a,b){
			return a.ocurs > b.ocurs ? -1 : a.ocurs < b.ocurs ? 1 : 0;
		});
		return handler(null,relevants);
	});

}

function getTopics(start,end,min_occurs,threshold,cluster,handler) {

	return getRelevantWordsByDate(start,end,min_occurs,function(err,relevants){
		if ( err )
			return handler(err,null);

		// Get all the articles involved
		var
			articleIDS = {};
		relevants.forEach(function(item){
			if ( item.ids == null )
				return;
			item.ids.forEach(function(id){
				articleIDS[id] = true;
			});
		});
		articleIDS = Object.keys(articleIDS);

		var
			wordsByArticle = {};

		// Get all words from refered articles
		console.log("Getting words of "+articleIDS.length+" referred articles...");
		var qstart = new Date();
		var numArts = 0;
		db.instance("default").find("wordsByDate",{id:{$in:articleIDS},word:{$ne:"_orig"},imp:1},{word:1,ocurs:1,id:1},function(err,items){
			if ( err )
				return handler(err,null);

			items.forEach(function(item){
				if ( wordsByArticle[item.id] == null )
					wordsByArticle[item.id] = [];
				wordsByArticle[item.id].push(item);
				numArts++;
			});

			console.log("Done. Got "+numArts+" words for "+articleIDS.length+" articles. Took "+(new Date()-qstart)+" ms");

			// For each seed
			relevants.forEach(function(seed){
				if ( !seed.ids )
					return;

				// For each document
				seed.ids.forEach(function(id){
					var words = wordsByArticle[id];
					if ( !words )
						return;	// Article not found

					// For each word on the document
					words.forEach(function(item){
						if ( !seed.linked )
							seed.linked = {};
						if ( seed.linked[item.word] == null )
							seed.linked[item.word] = item.ocurs;
						else
							seed.linked[item.word] += item.ocurs;
					});
				});
			});

			// Build top linked words
			relevants.forEach(function(seed){
				if ( !seed.linked )
					return;
				seed.topLinked = [];
				for ( var word in seed.linked ) {
					if ( seed.linked[word] > seed.ocurs - threshold && word != seed.word )
						seed.topLinked.push(word);
				}
				seed.topLinked = seed.topLinked.sort(function(a,b){
					return ( seed.linked[a] < seed.linked[b] ) ? 1 : ( seed.linked[a] > seed.linked[b] ) ? -1 : 0;
				});
//				delete seed.linked;
			});

			// Filter them (remove those who have seeds on topLinked)
			if ( cluster ) {
				var
					seedByWord = {},
					finalItems = [];
				relevants.forEach(function(seed){
					if ( seed.topLinked ) {
						var found = null;
						for ( x = 0 ; x < seed.topLinked.length ; x++ ) {
							var word = seed.topLinked[x];
							if ( seedByWord[word] ) {
								found = seedByWord[word];
								break;
							}
						}
						if ( found == null ) {
							finalItems.push(seed);
							seedByWord[seed.word] = seed;
						}
						else {
							var allLinked = {};
							found.topLinked.forEach(function(word){
								if ( word != found.word )
									allLinked[word] = true;
							});
							allLinked[seed.word] = true;
							found.topLinked = Object.keys(allLinked);
							if ( !found.clustered )
								found.clustered = 1;
							else
								found.clustered++;
						}
					}
				});
			}
			else
				finalItems = relevants;

			return handler(null,finalItems);
		});

	});

}

// Get contents
function getContents(ids,handler) {

	console.log("Getting contents for "+ids.length+" ids...");
	var qstart = new Date();
	return db.instance("default").find("contents",{id:{$in:ids}},{},{sort:{DateTime:1}},function(err,contents){
		if ( err )
			return handler(err,null);
		console.log("Done. Got "+contents.length+" contents. Took "+(new Date()-qstart)+" ms");
		return handler(null,contents);
	});

}
