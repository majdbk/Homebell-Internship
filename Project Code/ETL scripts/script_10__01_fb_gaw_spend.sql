


DELETE FROM facts.marketing
WHERE fact_subtype = 'mkt_spend' 
AND date_id >= (SELECT MIN(f_TO_DID(date)) FROM scratch.mkt_spend);
INSERT INTO facts.marketing 
(
	-- basic
	event_at,fact_type,fact_subtype,
	-- forgein keys
	session_id,user_id,lead_id,date_id,session_did,utm_id,
	geo_id,marketed_vertical_id,vertical_type_id,
	-- specific
	cost,impressions,clicks,

	pbi_utm_id
)
SELECT

	date::timestamp AS event_at,
	'cost' AS fact_type,
	'mkt_spend' AS fact_subtype,
	'na' AS session_id,
	'na' AS user_id,
	0 AS lead_id,
	f_TO_DID(date) AS date_id,
	0 AS session_did,
	sp.utm_id,
	sp.geo_id,
	ch.vertical_type_id AS marketed_vertical_id,
	0 AS vertical_type_id,
    sp.cost,
    sp.impressions,
    sp.clicks,
    
    COALESCE(ch.pbi_utm_id,1) AS pbi_utm_id
FROM scratch.mkt_spend sp
LEFT JOIN dims.channels ch on ch.utm_id = sp.utm_id;





