

DROP TABLE IF EXISTS refined.onsite_conversions;
CREATE TABLE refined.onsite_conversions 
SORTKEY(tr_orderid)
AS
(
	WITH load_start AS (
		SELECT Coalesce(max(logged_at),'1970-01-01'::timestamp) AS start
		FROM sys.monitor_loading
		WHERE runner_title = 'events' AND position_type = 0
	),
	prep AS (
		SELECT 
			a.event_id, 
			a.event_fingerprint, 
			a.event, 
			a.collector_tstamp, 
			a.tr_orderid, 
			smp.session_id, 
			a.domain_userid, 
			COALESCE(//um.onsite_user_id,a.domain_userid) AS onsite_user_id, 
			COALESCE(//um.login_user_id,a.domain_userid) AS login_user_id,
			ROW_NUMBER() OVER (PARTITION BY a.tr_orderid ORDER BY a.collector_tstamp) as ranking
		FROM atomic.events a 
		INNER JOIN scratch.event_session_map smp  ON smp.event_id = a.event_id AND smp.event_fingerprint = a.event_fingerprint
  		//LEFT JOIN scratch.sp_user_id_mapping um ON um.domain_userid = a.domain_userid
		WHERE a.collector_tstamp < (SELECT start FROM load_start) AND tr_orderid IS NOT NULL
		AND a.event = 'tr' 
		AND a.tr_orderid IN (SELECT lead_id::VARCHAR FROM refined.valid_leads)
		-- these two should not happen as neither bots nor internal should be counted as internal lead gen
		--AND smp.is_internal = FALSE AND smp.is_bot = FALSE
		AND a.app_id NOT IN ('web-localhost','web-staging') 
	)
	SELECT *,
	'LEAD'::varchar as conversion_type,
	('LEAD-'||session_id)::varchar as conversion_path_id,
	convert_timezone('Europe/Berlin',collector_tstamp) as onsite_conversion_locale_tstamp
	FROM prep 
	WHERE ranking = 1
);