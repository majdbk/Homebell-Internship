
-- EVENT SESSION MAPPING PART A (stage)
-- reference  http://discourse.snowplowanalytics.com/t/reconciling-snowplow-and-google-analytics-session-numbers/80
-- ways to be a new session
-- 1) timeout  - last event was 1800 secs ago (this is the default domain_sessionid split)
-- 2) new day - last session was date_trunc differnt
-- 3) refer + mkts is not the same as current session refr

<%
	var recreate = locals.recreate || false;
%>
DROP TABLE IF EXISTS scratch.event_session_map<%if(!recreate){%>_stage<%}%> ;
CREATE TABLE scratch.event_session_map<%if(!recreate){%>_stage<%}%> 
	DISTKEY (event_id)
	SORTKEY (collector_tstamp)
AS (
	WITH window AS (
		SELECT 
			(SELECT 
<% if(recreate){%>
				'1970-01-01'::timestamp
<%}else{%>
				f_BERLIN_DAY_START(
					COALESCE(MAX(collector_tstamp),'1970-01-01'::timestamp)
				)
				FROM scratch.event_session_map
<%}%>
			) AS start_at,
			(
				SELECT MAX(logged_at) FROM sys.monitor_loading
				WHERE job_title = 'events_start'
			) AS end_at
	),
	prep AS (
		SELECT 
			event_id, 
			event_fingerprint, 
			collector_tstamp,
			domain_userid,
			domain_sessionid,
			--
			TO_CHAR(
				DATE_TRUNC('day', CONVERT_TIMEZONE('UTC','Europe/Berlin',collector_tstamp)),
			'YYYYMMDD')::CHAR(8) AS locale_date,
			--
			CASE 
				WHEN refr_medium = 'internal' THEN NULL
				ELSE  MD5(LOWER(COALESCE(page_url,'')||COALESCE(page_referrer,'')))
			END AS entrance_id,
			ROW_NUMBER() OVER (PARTITION BY domain_userid ORDER BY collector_tstamp,derived_tstamp ASC) AS derived_ranking,
		    ROW_NUMBER() OVER ( PARTITION BY event_id,event_fingerprint ORDER BY collector_tstamp DESC) AS ranking
		FROM atomic.events a
		-- Discontinued as the tracker logs this INNER JOIN scratch.bot_agents bt ON a.useragent = bt.useragent AND bt.is_bot = FALSE
		WHERE collector_tstamp BETWEEN (SELECT start_at FROM window)  -- last update
	    AND (SELECT end_at FROM window) -- change of day in Berlin
    	AND domain_userid IS NOT NULL -- rare edge case
	    AND event_fingerprint IS NOT NULL
	    AND event_id IS NOT NULL
	    AND (CASE WHEN br_renderengine = 'bot' THEN FALSE ELSE TRUE END)
		AND (CASE WHEN app_id = 'web-internal' THEN FALSE ELSE TRUE END)
    ),
    session_break AS (
    	SELECT
	    	event_id::char(36), 
			event_fingerprint::varchar(128), 
			collector_tstamp,
			locale_date,
			domain_userid::char(36),
			entrance_id::char(36),
			derived_ranking,
			ROW_NUMBER() OVER (PARTITION BY domain_userid,domain_sessionid,locale_date ORDER BY derived_ranking ASC) AS session_ranking
		FROM prep
		WHERE ranking = 1
	),
	count_breaks AS (
		SELECT 
			*,
			COUNT(CASE WHEN session_ranking = 1 THEN event_id ELSE entrance_id END) 
			  OVER (PARTITION BY domain_userid ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS count_break
		FROM session_break
	),
	session_prep AS(
		SELECT 
			event_id::char(36), 
			event_fingerprint::varchar(128), 
			collector_tstamp,
			domain_userid::char(36),
			entrance_id::char(36),
			locale_date,
			derived_ranking,
			session_ranking,
			FIRST_VALUE( COALESCE(entrance_id,'direct_or_time') )
			OVER (PARTITION BY domain_userid,count_break ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS session_entrance_id
		FROM count_breaks
	),
	session_id_prep AS 
	(
		SELECT 
			event_id,
			event_fingerprint,
			collector_tstamp,
			domain_userid,
			derived_ranking,
			entrance_id,
			CASE 
				WHEN session_ranking = 1 OR
				LAG(session_entrance_id) OVER (PARTITION BY domain_userid ORDER BY derived_ranking ASC) != COALESCE(session_entrance_id,'')
			THEN locale_date||'_'||event_id
			END AS session_id_prep
		FROM session_prep
	),
	session_spread AS (
		SELECT 
			event_id,
			event_fingerprint,
			collector_tstamp,
			domain_userid,
			derived_ranking,
			session_id_prep::CHAR(45),
			entrance_id IS NOT NULL AS is_external,
			COUNT(session_id_prep) OVER (PARTITION BY domain_userid ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING) AS count_session_id
		FROM session_id_prep
	)
	SELECT
		FIRST_VALUE(session_id_prep) 
			OVER (PARTITION BY domain_userid,count_session_id ORDER BY derived_ranking ASC ROWS UNBOUNDED PRECEDING)
		AS session_id,
		event_id,
		event_fingerprint,
		collector_tstamp,
		domain_userid,
		derived_ranking,
		ROW_NUMBER() 
			OVER (PARTITION BY domain_userid,count_session_id ORDER BY derived_ranking ASC)
		AS session_event_index,
		is_external
	FROM session_spread
);
<%if(!recreate){%>
DELETE FROM scratch.event_session_map
WHERE collector_tstamp >= (
	SELECT MIN(collector_tstamp)
	FROM scratch.event_session_map_stage
);

INSERT INTO scratch.event_session_map
SELECT * FROM scratch.event_session_map_stage;
<% } %>








