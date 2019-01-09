const runner = require('../services/sqlRunner');
const fs = require('fs');
async function run()
{
	let json = 'events_rebuild';// starts at 2
	let startAt = process.argv[2] || 0;
	startAt = parseInt(startAt);
	let endAt = process.argv[3];
	if(!json) 
	{
		throw new Error('JSON runner File required as Argument');
	}
	json = /\.json$/.test(json) ? json : json+'.json';
	let file = await runner.config.getFile(json)
	file = JSON.parse(file);
	process.env.LOCAL = 'LOCAL LOAD';
	console.log('------------------------');
	console.log('Running Build : '+json);
	console.log('------------------------');

	try{
		file.sequence = file.sequence.map(function(p){
			if(/01_events|02_marketing/.test(p.path)){
				p.params = p.params || {}
				if(p.params.recreate === undefined){
					p.params.recreate = true
				}
			}
			return p;
		});
		const options = {
			manifest:file,
			startAt:startAt,
			debug:true
		};
		if(endAt){
			options.endAt = parseInt(endAt)
		}
		console.log('OPTIONS',options);
		const res = await runner.run(options)
		console.log('------------------------');
		console.log("COMPLETED!");
		console.log('------------------------');

		console.log(res);
		process.exit(0);
	}catch(err){
		console.log(err)
		process.exit(1);
	}
}
run();