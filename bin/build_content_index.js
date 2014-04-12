#!/usr/bin/env node

var
	async = require('async'),
	fs = require('fs'),
	db = require('../lib/db'),
	n = -1;

fs.readdir("../old/words_js/pt/",function(err,files){
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
			return fs.readFile("../old/articles_js/"+id, function (err, data) {
				n++;
				if ( n && ((n % 1000) == 0) )
					console.log("Registered "+n+" items");

				if (err) throw err;
				var j = JSON.parse(data.toString());
				var doc = { id: id, Title: j.Title, URL: j.URL, Description: j.Description };
				if ( j.Photos && j.Photos.length > 0 )
					doc.Image = j.Photos[0].URL;
				if ( j.Producer && j.Producer.Name )
					doc.Source = j.Producer.Name;
				if ( j.LinkedContent ) {
					if ( j.LinkedContent.Categories )
						doc.Categories = j.LinkedContent.Categories;
					if ( j.LinkedContent.DateTime )
						doc.DateTime = j.LinkedContent.DateTime;
				}
				if ( j.History && j.History.ActivePeriod ) {
					doc.Start = j.History.ActivePeriod.StartDate;
					doc.End = j.History.ActivePeriod.EndDate;
				}
				var docs = [doc];

				return db.instance("default").insert("contents",docs,function(err,ok){
					if ( err ) throw err;
					return next(null,true);
				});
			});
		},
		function(err,ok){
			console.log("Done");
			process.exit(0);
		}
	);
});
