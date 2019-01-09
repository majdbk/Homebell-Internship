
<%
	var recreate = locals.recreate || false;
%>
DROP TABLE IF EXISTS scratch.sessions_prep;
CREATE TABLE scratch.sessions_prep 
	DISTKEY(start_event_id)
	SORTKEY(start_collector_tstamp)
AS
(
	WITH window AS (
		SELECT 
			(SELECT 
<% if(recreate){%> '1970-01-01'::timestamp
<%}else{%>
				f_BERLIN_DAY_START(
					COALESCE(MAX(start_collector_tstamp),'1970-01-01'::timestamp)
				)
				FROM refined.sessions
<%}%>
			) AS start_at,
			(
				SELECT MAX(logged_at) FROM sys.monitor_loading
				WHERE job_title = 'events_start'
			) AS end_at
	)
		SELECT DISTINCT
	  		s.session_id,
	  		s.domain_userid,
	  		FIRST_VALUE(a.event_id) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS start_event_id,
	  		FIRST_VALUE(a.event_fingerprint) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS start_event_fingerprint,
	  		FIRST_VALUE(a.collector_tstamp) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS start_collector_tstamp,
	  		LAST_VALUE(a.event_id) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS end_event_id,
	  		LAST_VALUE(a.event_fingerprint) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS end_event_fingerprint,
	  		LAST_VALUE(a.collector_tstamp) OVER (PARTITION BY s.session_id,s.domain_userid ORDER BY derived_ranking ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS end_collector_tstamp,
	  		COUNT(a.event_id) OVER (PARTITION BY s.session_id,s.domain_userid) AS events,
	  		SUM(CASE WHEN a.event = 'transaction' THEN 1 ELSE 0 END) OVER (PARTITION BY s.session_id,s.domain_userid) AS transactions,
	  		SUM(CASE WHEN a.event ='page_view' THEN 1 ELSE 0 END) OVER (PARTITION BY s.session_id,s.domain_userid) AS page_views
		FROM scratch.event_session_map s
	  	LEFT JOIN atomic.events a ON 
	  		s.collector_tstamp = a.collector_tstamp
	  		AND a.event_id = s.event_id 
	  		AND s.event_fingerprint = a.event_fingerprint 
	  	WHERE s.collector_tstamp > (SELECT start_at FROM window)  -- last update
	    	AND  s.collector_tstamp < (SELECT end_at FROM window) 
);