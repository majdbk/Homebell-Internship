


--DROP TABLE IF EXISTS scratch.adgroup_utm_map;
--CREATE TABLE scratch.adgroup_utm_map 
--SORTKEY(join_id)
--AS (

INSERT INTO scratch.adgroup_utm_map
WITH new_clicks AS (
	SELECT account_id||campaign_id||ad_group_id AS join_id,
		account_id,
		campaign_id, 
		campaign_name,
		ad_group_id,
		gcl_id,
		date
	FROM raw_data.gaw_click_report
	WHERE account_id||campaign_id||ad_group_id NOT IN (SELECT join_id FROM scratch.adgroup_utm_map)
	AND date > '2017-06-01'
),
prep AS (
	SELECT DISTINCT
		account_id||campaign_id||ad_group_id AS join_id,
		account_id,
		campaign_id, 
		ad_group_id, 
		LAST_VALUE(TRIM(LOWER(a.mkt_source)) IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as source,
		LAST_VALUE(TRIM(LOWER(a.mkt_medium)) IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as medium,
		LAST_VALUE(TRIM(LOWER(campaign_name)) IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as campaign,
		--campaign_name as campaign,
		LAST_VALUE(TRIM(LOWER(a.mkt_term)) IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as term,
		LAST_VALUE(TRIM(LOWER(a.mkt_content)) IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as content,
		LAST_VALUE(gaw.date IGNORE NULLS) OVER (PARTITION BY ad_group_id ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as click_date
	FROM new_clicks gaw
	INNER JOIN atomic.events a
	ON (gaw.date = DATE(a.collector_tstamp) OR gaw.date = DATE(convert_timezone('Europe/Berlin',a.collector_tstamp)) )
	AND gaw.gcl_id = a.mkt_clickid
	)
SELECT 
	source||medium||campaign||term AS marketing_string_id,
	ROW_NUMBER() OVER (PARTITION BY source||medium||campaign||term ORDER BY click_date DESC) as marketing_string_rank,
	*,
	f_decode_url( SPLIT_PART(content,'_',4) ) AS keyword
FROM prep
--)

