
<%
	var recreate = locals.recreate || false;
%>
<%if(recreate){%>
	DROP TABLE IF EXISTS refined.sessions;
	CREATE TABLE refined.sessions 
		DISTKEY(start_event_id)
		SORTKEY(start_collector_tstamp)
	AS
<%}else{%>
	DELETE FROM refined.sessions
	WHERE start_collector_tstamp >= (
		SELECT MIN(start_collector_tstamp) 
		FROM scratch.sessions_prep
	);
INSERT INTO refined.sessions
<%}%>
(
	WITH prep AS (
		SELECT 
			s.session_id,
			s.start_event_id,
			s.start_event_fingerprint,
			s.start_collector_tstamp,
			s.end_event_id,
		  	s.end_event_fingerprint,
		  	s.end_collector_tstamp,
			s.domain_userid,
			u.login_user_id,
			u.onsite_user_id,
			Convert_timezone('Europe/Berlin',a.collector_tstamp) AS start_locale_tstamp,
			f_TO_DID(Convert_timezone('Europe/Berlin',a.collector_tstamp)) AS start_did,
			-- if it was direct and during a peak give it to TV
			COALESCE(
					utm.utm_id,
					tv.utm_id,
				CASE 
				WHEN a.refr_medium = 'internal' THEN MD5('timeout')
					ELSE MD5('')
				END
			)::CHAR(32) AS utm_id,
			a.page_url as landing_page_url,
			a.page_title as landing_page_title,
			a.page_urlpath as landing_pagepath,
			-- session info (19-40)
			a.page_urlhost,
			a.page_referrer,
			a.mkt_medium,
			--a.geo_country,
			--a.geo_city,
			LOWER(COALESCE(c.iso_code,'00')) AS country,
			TRIM(LOWER(COALESCE(a.geo_city,''))) AS city, 
			a.geo_zipcode,
			a.geo_latitude,
			a.geo_longitude,
			a.geo_region_name,
			a.useragent,
			a.br_name,
			a.br_family,
			a.br_version,
			a.br_type,
			a.br_lang,
			a.br_features_pdf,
			a.br_viewwidth,
			a.br_viewheight,
			a.os_name,
			a.os_family,
			a.dvce_type,
			a.dvce_ismobile,
			a.dvce_screenwidth,
			a.dvce_screenheight,
			EXTRACT(epoch FROM end_collector_tstamp - start_collector_tstamp) AS duration_seconds,
			s.events < 2 AS is_bounce,
			(s.events < 2)::INT AS bin_bounce,
			s.events,
			s.transactions,
			s.page_views,
			ROW_NUMBER() OVER (PARTITION BY s.session_id) AS RANKING
		FROM scratch.sessions_prep s
		LEFT JOIN scratch.sp_user_id_mapping u ON u.domain_userid = s.domain_userid
		LEFT JOIN atomic.tv_peaks tv ON DATE_TRUNC('minute',s.start_collector_tstamp) = tv.minute
		LEFT JOIN atomic.events a ON 
		  		s.start_collector_tstamp = a.collector_tstamp
		  		AND s.start_event_id = a.event_id 
		  		AND s.start_event_fingerprint = a.event_fingerprint
		LEFT JOIN scratch.iso_country_codes c on LOWER(c.country) = LOWER(a.geo_country)
		LEFT JOIN scratch.channel_prep utm ON 
		  		s.start_collector_tstamp = utm.collector_tstamp
		  		AND s.start_event_id = utm.event_id 
		  		AND s.start_event_fingerprint = utm.event_fingerprint
		<%if(!recreate){%>
		WHERE session_id NOT IN (SELECT session_id FROM refined.sessions) AND session_id IS NOT NULL
		<% } %>
	)
	SELECT
		session_id,
		start_event_id,
		start_event_fingerprint,
		start_collector_tstamp,
		end_event_id,
	  	end_event_fingerprint,
	  	end_collector_tstamp,
		domain_userid,
		login_user_id,
		onsite_user_id,
		start_locale_tstamp,
		start_did,
		-- if it was direct and during a peak give it to TV
		p.utm_id,
		TRIM(LOWER(COALESCE(country,'')||COALESCE(city,''))) as geo_id,
		landing_page_url,
		landing_page_title,
		landing_pagepath,
		-- session info (19-40)
		page_urlhost,		
		country,
		city,
		--geo_country,
		--geo_city,
		geo_zipcode,
		geo_latitude,
		geo_longitude,
		geo_region_name,
		useragent,
		br_name,
		br_family,
		br_version,
		br_type,
		br_lang,
		br_features_pdf,
		br_viewwidth,
		br_viewheight,
		os_name,
		os_family,
		dvce_type,
		dvce_ismobile,
		dvce_screenwidth,
		dvce_screenheight,
		duration_seconds,
		is_bounce,
		bin_bounce,
		events,
		transactions,
		page_views,
  		u.id AS pbi_utm_id
	FROM prep p
	LEFT JOIN scratch.utm_ids u ON u.utm_id = p.utm_id
	WHERE RANKING = 1
);




