
<%
	var recreate = locals.recreate || false;
%>
<%
  var event_types = `nrw
niedersachsen
nuremberg
cologne
freiburg
münchen
rostock
kiel
münster
duisburg
magdeburg
karlsruhe
bremen
frankfurt
dresden
leipzig
kassel
braunschweig
berlin
augburg
mainz
muesnter
city-extended
baden
bayern
extended
muenster
duesseldorf
wiesbaden
munich
nurnberg
munchen
bonn
rostock
munster
dusseldorf
köln
bielefeld
koln
kiel
hannover
mannheim
essen
bochum
dortmund
augsburg
nürnberg
general
stuttgart
düsseldorf
hamburg`.split('\n').map(v=>v.trim());
%>
DROP TABLE IF EXISTS scratch.channel_prep;
CREATE TABLE scratch.channel_prep
	SORTKEY(utm_id)
AS
(	
	-- If there's a channels table it creates a window between the beginning of the last updated date and the last event logged
	WITH window AS (
		SELECT 
			(SELECT 
				<%if(!recreate) {%>
				f_BERLIN_DAY_START(
					COALESCE(MAX(updated_at),'1970-01-01'::timestamp)
				)
				FROM dims.channels
				<%}else{%>
					'1970-01-01'::timestamp
				<%}%>  
			) AS start_at,
			(
				SELECT MAX(logged_at) FROM sys.monitor_loading
				WHERE job_title = 'events_start'
			) AS end_at
	),
	-- Gives the UTM information for events that lie within the window created above 
	prep AS (
		SELECT 
			a.event_id,
			a.event_fingerprint,
			a.collector_tstamp,
			page_urlpath = '/' AS is_home_landing,
			refr_medium,
			CASE 
				WHEN a.mkt_source IS NULL AND refr_medium = 'email' THEN 'email'
				WHEN g.ad_group_id IS NOT NULL
				THEN DECODE(LOWER(g.ad_network_type1),'search network','google',LOWER(g.ad_network_type1))
				ELSE LOWER(a.mkt_source) 
			END AS mkt_source,
			CASE 
				WHEN g.ad_group_id IS NOT NULL THEN 'cpc' 
				ELSE f_LOWER_NOT_NULL(mkt_medium) 
			END AS medium,
			f_LOWER_NOT_NULL(COALESCE(mp.campaign,g.campaign_name,a.mkt_campaign) ) campaign,
			f_LOWER_NOT_NULL(COALESCE(g.ad_group_name,a.mkt_term)) term,
			f_LOWER_NOT_NULL(COALESCE(g.keyword_match_type,SPLIT_PART(a.mkt_term,'_',1))) match_type,
			f_LOWER_NOT_NULL(mkt_content) content,
			--COALESCE(mkt_clickid,'') AS clickid,
			mp.join_id,
		  	mp.account_id,
		  	COALESCE(mp.campaign_id,g.campaign_id) AS campaign_id,
		  	COALESCE(mp.ad_group_id,g.ad_group_id) AS ad_group_id,
			f_LOWER_NOT_NULL(CASE 
				WHEN refr_urlhost LIKE 'www.google%' THEN 'google'
				WHEN refr_urlhost LIKE 'www.pinterest%' OR refr_urlhost LIKE '__.pinterest%'  OR refr_urlhost LIKE 'pinterest' THEN 'pinterest'
				WHEN refr_urlhost LIKE 'www.facebook%' OR refr_urlhost LIKE '_.facebook%' OR refr_urlhost LIKE  '__.facebook%' OR  refr_urlhost LIKE '___.facebook%' THEN 'facebook'
				WHEN refr_urlhost LIKE 'images.google%' THEN 'images.google'
				WHEN refr_urlhost LIKE '_.instagram%' THEN 'instagram'
				WHEN refr_urlhost LIKE 'r.search.yahoo%' THEN 'yahoo'
				ELSE REPLACE(refr_urlhost,'www.','')
			END) AS refr_host,
	  		FALSE AS is_lead_provider
		FROM atomic.events a 
		LEFT JOIN raw_data.gaw_click_report g ON a.mkt_clickid = g.gcl_id AND g.date = DATE(a.collector_tstamp)
		LEFT JOIN scratch.adgroup_utm_map mp 
		  	ON marketing_string_rank = 1 AND (a.mkt_source||a.mkt_medium||a.mkt_campaign||a.mkt_term = mp.marketing_string_id ) --OR mp.ad_group_id = u.adgroup_id_from_utm)
		WHERE a.collector_tstamp 
		<%if(!recreate) {%>
			BETWEEN (SELECT start_at FROM window) AND 
		<%}else{%> 
			<=
		<%}%> (SELECT end_at FROM window)
		AND (CASE 
			WHEN --refr_medium = 'internal' 
				--OR refr_urlhost LIKE 'homebell%'
				--OR refr_urlhost LIKE '__.homeb%' 
				--OR refr_urlhost LIKE '___.homeb%'
				--OR refr_urlhost LIKE '__.homel%'
				--OR refr_urlhost LIKE 'partners.homeb%'
				--OR page_referrer  LIKE 'https://__.homeb%'
				--OR page_referrer  LIKE 'https://___.homeb%'
				--- SALESFORCE DOESNT COUNT
				refr_urlhost  LIKE 'eu6.sale%'
				OR refr_urlhost LIKE 'c.eu6.vis%'
				OR refr_urlhost LIKE 'c.cs84.vis%'
				OR refr_urlhost LIKE 'eu6.ligh%'
				OR refr_urlhost LIKE 'na39.sale%'
				OR refr_urlhost LIKE 'na35.sale%'

				OR page_referrer  LIKE 'https://www.paypal%'
				OR page_referrer LIKE 'hooks.stripe%'
				OR page_referrer  LIKE 'https://www.facebook.com/v2.2/dialog/oauth%'
				OR page_referrer  LIKE 'https://www.facebook.com/login.php%'
				OR page_referrer  LIKE 'https://m.facebook.com/login.php%'
				OR page_referrer  LIKE 'https://free.facebook.com/%'
			THEN FALSE
			ELSE TRUE
		END) = TRUE 
	)
<% 
	var utm_id = "COALESCE(mkt_source,'')||COALESCE(refr_host,'')||medium||campaign||term||content||CASE WHEN is_home_landing THEN 't' ELSE 'f' END";
%>
	-- Adds an MD5'd id for the utm among some other important information
	SELECT
		MD5(<%-utm_id%>)::CHAR(32) AS utm_id,
		CASE 
			WHEN is_lead_provider THEN 'lead-provider'
		WHEN SPLIT_PART(campaign,'_',2) IN ('de','be','nl') THEN 'utm-builder'
		WHEN SPLIT_PART(campaign,'_',1) IN ('de','be','nl') THEN 'old-system'
		ELSE 'other'
		END AS tagging_system,
		SPLIT_PART(campaign,'_',2) AS position_2,

		CASE WHEN SPLIT_PART(campaign,'_',5) = 'extended' 
			THEN 'extended'
			WHEN SPLIT_PART(campaign,'_',4) LIKE 'city-extended%' 
			THEN 'extended'
			WHEN campaign like '%extended%' and campaign like 'gaw%'
		    THEN 'extended'

		    <% event_types.forEach(function(col){%> 
		    WHEN (campaign LIKE '%city%' and campaign like 'gaw%') or (campaign like 'gaw%' and campaign like '%<%=col%>%')or (campaign like 'gaw%' and campaign like '%<%=col%>%' and campaign LIKE '%city%')
			THEN 'city'
			<%})%>
			<% event_types.forEach(function(col){%> 
		    WHEN campaign like 'gaw%' and (campaign not like '%city%' OR campaign NOT like '%<%=col%>%')
		    THEN 'non_city'
			<%})%>	
			

			ELSE 'na'
		END AS city_style,
		CASE WHEN SPLIT_PART(campaign,'_',2) = 'sn'
			THEN SPLIT_PART(campaign,'$',1) 
		END AS sn_camp,
		CASE WHEN campaign like 'gaw_de_partner%'
			THEN SPLIT_PART(term,'_',2)
		END as adgroup_id_from_utm,
		*,
		COALESCE(NULLIF(mkt_source,''),refr_host) AS source,
		ROW_NUMBER() OVER (
			PARTITION BY MD5(<%-utm_id%>) ORDER BY collector_tstamp DESC
		) AS ranking
	FROM prep
);






