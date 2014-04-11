#!/usr/bin/env node

var
	fs = require('fs'),
	http = require('http'),
	qs = require('querystring'),
	db = require('./lib/db');


// Read totals JSON

console.log("Reading totals..");
var totals = JSON.parse(fs.readFileSync("totals.json").toString());
var totalKeys = Object.keys(totals).length;
var totalOcurs = 0;
for ( var k in totals )
	totalOcurs += totals[k];
console.log("totals: "+totalKeys+" / "+totalOcurs);


http.createServer(function (req, res) {
	if ( req.url && req.url.match(/\?(.*)$/) ) {
		var args = RegExp.$1;
		req.args = qs.parse(args);
		req.url = req.url.replace(/\?.*$/,"");
	}
	if ( !req.args )
		req.args = {};


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
			res.end(JSON.stringify(data));
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
			res.end(JSON.stringify(relevants,null,4));
		});
	}
	else if ( req.url == "/getTopics" ) {

		if ( !req.args.start || !req.args.end ) {
			res.writeHead(500, {'Content-Type': 'text/plain'});
			res.end('Manda start e end carago!');
			return;
		}
		req.args.start = parseInt(req.args.start) * 1000000;
		req.args.end = parseInt(req.args.end) * 1000000 + 1;
		req.args.occurs = parseInt(req.args.occurs) || 10;
		return getTopics(req.args.start,req.args.end,req.args.occurs,function(err,topics){
			if ( err ) {
				res.writeHead(500, {'Content-Type': 'text/plain'});
				res.end('Erro a obter os topicos');
				return;
			}
			res.writeHead(200, {'Content-Type': 'text/plain; charset=utf-8'});
			res.end(JSON.stringify(topics,null,4));
		});

	}

}).listen(8080, '0.0.0.0');
console.log('Server running at http://0.0.0.0:8080/');




// Functions

function getWordsByDate(start,end,handler) {

	return db.instance("default").find("wordsByDate",{date:{$gt:start,$lt:end},where:{$in:["Title","Description"]}},{word:1,ocurs:1,id:1},function(err,data){
		if ( err ) {
			console.log("A query deu errors: ",err);
			return handler(err,null);
		}
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
			retList.push(d);
			delete d['_id'];
		});

		// Convert ids to Array
		retList.forEach(function(i){
			if ( i.ids )
				i.ids = Object.keys(i.ids);
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

function getTopics(start,end,min_occurs,handler) {

	return getRelevantWordsByDate(start,end,min_occurs,function(err,relevants){
		if ( err )
			return handler(err,null);

		// Get all the articles involved
		var articleIDS = {};
		relevants.forEach(function(item){
			if ( item.ids == null )
				return;
			item.ids.forEach(function(id){
				articleIDS[id] = true;
			});
		});

		// Get all words from refered articles
		

	});

}
