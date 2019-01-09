const https = require('https');
const request = require('request');
const auth = require('./oAuth');
const HOMEBELL_GROUP = '7062de18-ab82-40f4-bafc-5b19d8b1d3b5'
const { authReq ,updateAccessToken,getAuthUri } = auth;
// video https://channel9.msdn.com/Shows/Data-Exposed/Setting-up-and-Getting-Started-with-Power-BI-Embedded
 // API REFERENCE https://msdn.microsoft.com/en-us/library/mt147898.aspx
const OBJECT_TYPES = {
	datasets:[],
	tables:['datasets'],
	rows:['datasets','rows'],
	reports:[],
	dashboards:[],
	tiles:['dashboards'],
}
async function getEmbedToken(params){
	let { group = HOMEBELL_GROUP, id, type, accessLevel , agentEmail, datasets } = params;
	let typeParam = type.substring(0,type.length-1)+'Id'
	let usedMap = {
		groupId:group,
		[typeParam]:id
	};
	let str = '';
	let url = `https://app.powerbi.com/${typeParam.replace('Id','')}Embed?groupId=`+group
	OBJECT_TYPES[type].forEach(function(parent) {
		let id = params[parent];
		let param = parent.substring(0,parent.length-1)+'Id'
		usedMap[param] = id
		str += `/${parent}/${id}`;
		url += param+'='+id+'&'
	});
	str += `/${type}/${id}`;
	url += typeParam+'='+id;
	
	let options = {
		path:`/groups/${group}${str}/GenerateToken`,
		TYPE:'post',
		body:{
		  "accessLevel": accessLevel || "View"
		}
	};
	if(agentEmail && type == 'reports'){
		let report = await getReports({id})
	    options.body.identities = [     
	        {      
	            "username": agentEmail,
	            "roles": [ "Agent" ],
	            "datasets": [ report.datasetId ]
	        }   
	    ] 
	};
	return authReq(options).then(function(res){
		return {
			type:/dashboard|tile/.test(type) ? 'dashboard' : 'report',
			id,
			token:res.token,
			params:usedMap,
			url
		}
	})
}
function getDashboards(params){
	params = params || {};
	let { group = HOMEBELL_GROUP, id } = params;
	let path = `/groups/${group}/dashboards`;
	if(id) path = path +'/'+id;
	return authReq({
		path,
		TYPE:'get'
	})
}
function getDatasets(params){
	params = params || {};
	let { group = HOMEBELL_GROUP, id } = params;
	let path = `/groups/${group}/datasets`;
	if(id) path = path +'/'+id;
	return authReq({
		path,
		TYPE:'get'
	})
}
function getReports(params){
	params = params || {};
	let { group, id } = params;
	group = group || HOMEBELL_GROUP;
	let path = `/groups/${group}/reports`;
	if(id) path = path +'/'+id;
	return authReq({
		path,
		TYPE:'get'
	})
}
function getTables(params){
	let { group, dataset, id } = params;
	group = group || HOMEBELL_GROUP;
	let path = `/groups/${group}/datasets/${dataset}/tables`;
	if(id) path = path +'/'+id;
	return authReq({
		path,
		TYPE:'get'
	})
}
function addRows(params) {
	let { group, dataset, table, rows } = params;
	group = group || HOMEBELL_GROUP;
	table = table || 'RealTimeData' // this is the default by POWER BI
	return authReq({
		path:`/groups/${group}/datasets/${dataset}/tables/${table}/rows`,
		TYPE:'post',
		body:{
			rows
		}
	})
}
function deleteRows(params) {
	let { group, dataset, table, rows } = params;
	group = group || HOMEBELL_GROUP;
	table = table || 'RealTimeData'
	return authReq({
		path:`/groups/${group}/datasets/${dataset}/tables/${table}/rows`,
		TYPE:'delete'
	})
}
function upsertRows(params){
	return deleteRows(params).then(res=>addRows(params))
}
module.exports = {
	updateAccessToken,
	getAuthUri,
	getEmbedToken,
	upsertRows,
	addRows,
	deleteRows,
	getTables,
	getDatasets,
	getReports,
	getDashboards
}











