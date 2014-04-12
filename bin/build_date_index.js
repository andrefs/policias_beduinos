#!/usr/bin/env node

var
	async = require('async'),
	fs = require('fs'),
	db = require('../lib/db'),
	n = -1;

fs.readdir("words_js/pt/",function(err,files){
	if ( err ) {
		console.log("Error getting file list: ",err);
		return;
	}

	console.log("files: ",files);
	async.mapSeries(files,
		function(file,next){

			if ( !file.match(/^(\d{8})T(\d{6})Z?_(\w+)/) )
				throw new Error("Cannot recognize file name pattern: "+file);
			var time = RegExp.$1+RegExp.$2;
			var id = RegExp.$3;
			return fs.readFile("words_js/pt/"+file, function (err, data) {
				n++;
				if ( n && ((n % 1000) == 0) )
					console.log("Registered "+n+" items");

				if (err) throw err;
				var j = JSON.parse(data.toString());
				var docs = [];
				for ( var f in j ) {
					for ( var word in j[f] ) {
						var doc = { where: f, word: word, ocurs: j[f][word], date: parseFloat(time), id: id };
						docs.push(doc);
					}
				}
				if ( docs.length > 0 ) {
					return db.instance("default").insert("wordsByDate",docs,function(err,ok){
						if ( err ) throw err;
						return next(null,true);
					});
				}
				return next(null,false);
			});
		},
		function(err,ok){
			console.log("Done");
			process.exit(0);
		}
	);
});
