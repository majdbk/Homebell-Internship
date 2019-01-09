
DROP TABLE IF EXISTS refined.sheets_mkt_spend;
CREATE TABLE refined.sheets_mkt_spend AS
(
	SELECT

	f_TO_DID(date) AS date_id,
	date::timestamp AS event_at,
	month_spend as month_cost,
	spend as cost,
	(case
	 when marketing_channel = 'paid search' then 'paid search'
	 when marketing_channel = 'native' then 'native'
	 when marketing_channel = 'retargeting' then 'display remarketing'
	 else 'undefined'
	END)::VARCHAR(22) as channel,
	
	(case
	 when source = 'yahoo' then 'yahoo'
	 when source = 'taboola' then 'taboola'
	 when source = 'ligatus' then 'ligatus'
	 when source = 'bing' and channel ='paid search' then 'paid search bing '
	 when source = 'smt' and channel ='display remarketing' then 'display retargeting sociomantic'
	else 'undefined'
	END) ::VARCHAR(32) as subchannel,
	
	source::VARCHAR(128) as source,
	'de' as geo_id,

	MD5 (channel||subchannel||source) AS utm_id


FROM scratch.sheets_mkt_spend sp
);
