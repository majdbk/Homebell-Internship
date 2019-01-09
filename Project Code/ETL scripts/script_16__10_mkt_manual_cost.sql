


DELETE FROM facts.marketing
WHERE fact_subtype = 'mkt_spend_manual';
INSERT INTO facts.marketing 
(
	-- basic
	event_at,fact_type,fact_subtype,
	-- forgein keys
	session_id,user_id,lead_id,date_id,session_did,
	geo_id,vertical_type_id,
	-- specific
	cost,
	utm_id,
	pbi_utm_id
)
SELECT

	event_at::timestamp AS event_at,
	'cost' AS fact_type,
	'mkt_spend_manual' AS fact_subtype,
	'na' AS session_id,
	'na' AS user_id,
	0 AS lead_id,
	date_id AS date_id,
	0 AS session_did,
	--sp.utm_id,
	geo_id as geo_id,
	--ch.vertical_type_id AS marketed_vertical_id,
	0 AS vertical_type_id,
    sp.cost,
    sp.utm_id as utm_id,
    --sp.impressions,
    --sp.clicks,
    
    COALESCE(ch.pbi_utm_id,1) AS pbi_utm_id
FROM refined.sheets_mkt_spend sp
LEFT JOIN dims.channels ch on ch.utm_id = sp.utm_id;





