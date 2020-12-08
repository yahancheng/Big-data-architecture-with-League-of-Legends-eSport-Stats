'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

hclient.table('yahancheng_final_combine').row('Blue_0_0_0_0_10').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

app.use(express.static('public'));
app.get('/profession.html',function (req, res) {
	const userInputRow= [req.query['side'], req.query['first_blood'], req.query['first_dragon'],
		req.query['first_tower'], req.query['first_baron']].join('_');

	function processPatchRecord(patchRecord) {
		var result = { patch : patchRecord['patch']};
		["winning_rate", "KPM", "DPM", "Kills", "Death", "Assist", "challenger", "grandmaster", "master"].forEach(result_type => {

			function winning_rate() {
				var total_games = patchRecord["totalGames"];
				var winning_games = patchRecord["winningGames"];
				if(total_games == 0)
					return " - ";
				return (((winning_games/total_games)*100).toFixed(2)).toString() + "%";
			}
			function avg_per_min(type) {
				var total_games = patchRecord["totalGames"];
				var APM = patchRecord["totalAvg"+type];
				if(total_games == 0)
					return " - ";
				return (APM/total_games).toFixed(2);
			}
			function total_KDA(type) {
				var total_games = patchRecord["totalGames"];
				var APM = patchRecord["total"+type];
				if(total_games == 0)
					return " - ";
				return (APM/total_games).toFixed(2);
			}
			function high_elo_winning_rate(type) {
				if (patchRecord[type+"TotalGames"] == null){
					return "-"
				}
				var total_games = patchRecord[type+"TotalGames"];
				var winning_games = patchRecord[type+"WinningGames"];
				console.log(total_games, winning_games)
				return (((winning_games/total_games)*100).toFixed(2)).toString() + "%";
			}

			if (result_type == 'winning_rate') {
				result[result_type] = winning_rate()
			} else if (result_type == 'KPM' || result_type == 'DPM') {
				result[result_type] = avg_per_min(result_type)
			} else if (result_type == "Kills" || result_type == "Death" || result_type == "Assist"){
				result[result_type] = total_KDA(result_type)
			} else {
				result[result_type] = high_elo_winning_rate(result_type)
			};
		})
		return result;

	}

	function gameInfo(cells) {
		var result = [];
		var patchRecord;
		cells.forEach(function(cell) {
			var patch = Number(removePrefix(cell['key'], userInputRow+"_"))
			if(patchRecord === undefined)  {
				patchRecord = { patch: patch }
			} else if (patchRecord['patch'] != patch ) {
				result.push(processPatchRecord(patchRecord))
				patchRecord = { patch: patch }
			}

			cell['$'] = counterToNumber(cell['$'])

			patchRecord[removePrefix(cell['column'],'game:')] = Number(cell['$'])
			console.log("line 118", patchRecord)
		})
		result.push(processPatchRecord(patchRecord))
		return result;
	}

	hclient.table('yahancheng_final_combine').scan({
			filter: {type : "PrefixFilter",
				value: userInputRow},
			maxVersions: 1},
		(err, cells) => {
			var gi = gameInfo(cells);
			var template = filesystem.readFileSync("result.mustache").toString();
			var html = mustache.render(template, {
				gameInfo : gi,
				game : userInputRow
			});
			res.send(html)
		})
});

/* Send simulated game data to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/game.html',function (req, res) {
	var side = req.query['side'];
	var first_blood = parseInt(req.query['first_blood']);
	var first_tower = parseInt(req.query['first_tower']);
	var first_dragon = parseInt(req.query['first_dragon']);
	var first_baron = parseInt(req.query['first_baron']);
	var winning_game = parseInt(req.query['winningGame']);
	var dpm = parseInt(req.query['avg_DPM']);
	var team_kpm = parseInt(req.query['avg_KPM']);
	var total_kills = parseInt(req.query['total_kills']);
	var total_death = parseInt(req.query['total_death']);
	var total_assist = parseInt(req.query['total_assist']);
	var patch = parseInt(req.query['patch']);

	var report = {
		side : side,
		firstblood : first_blood,
		firsttower : first_tower,
		firstdragon : first_dragon,
		firstbaron : first_baron,
		result : winning_game,
		dpm : dpm,
		team_kpm : team_kpm,
		total_kills : total_kills,
		total_death : total_death,
		total_assist : total_assist,
		patch : patch
	};

	kafkaProducer.send([{ topic: 'yahancheng_profession-lol', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log("Kafka Error: " + err)
			console.log(data);
			console.log(report);
			res.redirect('submit-game.html');
		});
});

app.listen(port);
