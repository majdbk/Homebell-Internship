const env = require('../../envs').get
const request = require('request');
const RESOURCE = "https://analysis.windows.net/powerbi/api";
const Keys = require('../../models/Keys');
const KEY = 'POWER_BI_API';
let CONFIG = env(KEY);
let ACCESS = Object.assign({},CONFIG.ACCESS_TOKEN) // we store an access token so that we have a refresh token
const BI_API = 'https://api.powerbi.com/v1.0/myorg';
///// https://docs.microsoft.com/en-gb/azure/active-directory/develop/active-directory-protocols-oauth-code
const login_uri = `https://login.microsoftonline.com/${CONFIG.tenant_id}/oauth2`;

function getAuthUri(){ // this is used to do a oAuth flow which we then complete with getAccessToken and save the end result
	const {client_id , redirect} = CONFIG;
	return login_uri+`/authorize?client_id=${client_id}`+
	`&response_type=code&redirect_uri=${encodeURIComponent(redirect)}`+
	`&response_mode=query&resource=${encodeURIComponent(RESOURCE)}&state=12345`
};
async function updateAccessToken(params){
	const res = await getAccessToken(params);
	let config = Object.assign({},CONFIG,{ACCESS_TOKEN:res});
	await Keys.updateOne(KEY,{value:config});
	return res;
}
function getAccessToken(params){
	let { code ,  refresh_token } = params;
	let grant_type = (code != undefined) ? 'authorization_code' : 'refresh_token';
	const {client_id , redirect, client_secret} = CONFIG;
	const options = {
		url:login_uri+'/token',
	    form:{
			grant_type,
			client_id,
			refresh_token,
			code,
			redirect_uri:redirect,
			resource:RESOURCE,
			client_secret
		}
	};
	return new Promise(function(resolve,reject) {
		request.post(options,function (err, httpResponse, body) {
			if(err) return reject(err)
			try{
				let response = JSON.parse(body)
				if(response.error){
					return reject(response)
				}
				return resolve(response)
			}catch(error){
				return reject({
					error,
					response:body
				})
			}
		})
	})
};
async function refreshToken() {
	let {value} = await Keys.findOne(KEY);
	CONFIG = value;
	ACCESS = CONFIG.ACCESS_TOKEN;
	let params = {
		refresh_token:ACCESS.refresh_token
	};
	const res = await updateAccessToken(params);
	ACCESS = res;
	return res;
}
function authReq(options,i){
	i = i || 0;
	i++;
	const {TYPE,path,body} = options;
	let params = {
		url:`${BI_API}${path}`,
		//`/groups/7062de18-ab82-40f4-bafc-5b19d8b1d3b5/datasets/a65ac55b-c413-4cf9-9a0f-137934174e7a/tables/RealTimeData/rows`,
		headers:{
			Authorization:`Bearer ${ACCESS.access_token}`
		}
	};
	if(body){
		params.body = body;
  		params.json = true;
	}
	if(options.debug ) console.log(TYPE,params.url)
	return new Promise(function(resolve,reject) {
		request[TYPE](params,function (err, httpResponse, body) {
			if(err) return reject(err);
			const statusCode = httpResponse.statusCode;
			if(statusCode != 403 && (!body || body == '') ){
				return (statusCode == 200) ? resolve('SUCCESS') : reject(statusCode)
			};
			if(typeof body == 'string' && body !==''){
				try{
					body = JSON.parse(body)
				}catch(error){
					console.log(error,body,statusCode);
					return reject({
						error,
						response:body
					});
				}
			}
			if(statusCode == 403 || ( body.error && body.error.code == 'TokenExpired') ){
				if(i > 3){
					return reject({error:'AUTH_FAILED',statusCode,body})
				}
				return refreshToken().then(token=>{
					return resolve( authReq(options,i) )
				});
			}else if(body.error){
				return reject(body.error)
			}
			return resolve(body);
		})
	})
}

module.exports = {
	getAccessToken,
	authReq,
	refreshToken,
	updateAccessToken,
	getAuthUri
}









