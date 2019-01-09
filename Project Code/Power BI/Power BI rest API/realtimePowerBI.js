const {Query:Queries} = require("../mongoose");
const { upsertRows } = require('../services/powerBI/index');
const logger = require('../services/logger')(__filename);
const {replaceDataset} = require('../services/gekoboards')
const moment = require('moment');
// https://api.powerbi.com/beta/ec901139-df4b-4bd4-a66c-0fc57cb2b1d9/datasets/a65ac55b-c413-4cf9-9a0f-137934174e7a/rows?key=hQnOB4t3W1urta7L4JDRJtE%2FHdHDvsK%2FGwAn1bmhyxHbySoxrALWGQZ71NnDC755GilSiHtNH2sd4GBy23EZyA%3D%3D
async function updateRealtime(){
	let minute = moment().minute();
	minute = (minute % 5 !== 0) ? 5 : minute;
	let minutes = [5,10,30,60].filter(function(k){
		return minute%k === 0
	}).join('|');
	let reg = new RegExp(`^(powerbi|geckoboard)-api-(${minutes})`,'i');
	let queries = await Queries.find({
		title:reg,
		pbi_dataset_id:{$ne:null}
	});
	let results = []
	for(query of queries){
		let rows = [];
		let { pbi_dataset_id, title }  = query;
		try{
			rows = await query.run();
			let loaded_at = new Date();
			rows = rows.map(r=>{
				r.loaded_at = loaded_at
				return r
			});
			let res = ''
			if(/^powerbi/.test(title)){
				res = await upsertRows({dataset:pbi_dataset_id,rows})
			}else{
				res = await replaceDataset({id:pbi_dataset_id,data:rows})
			} 
			results.push({
				rowsProcessed:rows.length,
				table:title
			});
		}catch(e){
			results.push({
				error:e,
				rowsProcessed:rows.length,
				table:title
			});
		}
	}
	logger(results.map(t=>{
		return {
			"event_type" :(t.error) ? "error" : "event",
			"event_category" :"Realtime PBI Push",
			"event_text" :`Realtime PBI Push ${t.error ? 'Error':''}`,
			"is_error" :(t.error) ? 1 : 0,
			"rows_processed" :t.rowsProcessed,
			"is_etl" :1,
			"rs_table" :t.table,
			"stack_trace" :(t.error) ? t.error.stack  : 'na',
			"error_message" :(t.error) ? t.error.message || t.error : 'na'
		}
	}));
	return results;
};
module.exports = updateRealtime

