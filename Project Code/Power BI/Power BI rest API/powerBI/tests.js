const core = require('./core');

core.getDashboards({
	//id: '0fe2f692-c9f1-4945-bdfc-aa19ee97108d'
}).then(res=>console.log(res)).catch(E=>console.log(E))