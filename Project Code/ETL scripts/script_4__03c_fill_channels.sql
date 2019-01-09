

-- Different source tables can be used to create the channels table, default is scratch.channel_prep
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

<%
	var recreate = locals.recreate || false;
	var sourceTable = sourceTable || 'scratch.channel_prep'
%>

-- We either recreate the whole table or just update the fields contained in the chosen source table
<%if(!recreate) {%>
DELETE FROM dims.channels 
WHERE utm_id IN (SELECT utm_id FROM <%=sourceTable %>);

INSERT INTO dims.channels
<%}else{%> 
DROP TABLE IF EXISTS dims.channels;
CREATE TABLE dims.channels 
	SORTKEY(utm_id)
	DISTKEY(utm_id)
AS
<%}%>

(
	WITH prep AS (
		SELECT 
	 	  p.*,
		  CASE 
		  	WHEN is_lead_provider THEN 'na'
		  	WHEN tagging_system = 'old-system' THEN SPLIT_PART(campaign,'_',2) 
		  	ELSE SPLIT_PART(campaign,'_',1) 
		  END AS prefix,
		  CASE
		  	WHEN is_lead_provider THEN 'na'
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(campaign,'_',2) 
		  	WHEN tagging_system = 'old-system' THEN SPLIT_PART(campaign,'_',1)  
		  	ELSE 'na'
		  END AS locale,
		   CASE
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(campaign,'_',4)  
		  	ELSE 'na'
		  END AS campaign_type,
		  CASE
		  	WHEN SPLIT_PART(campaign,'_',1) = 'b' OR position_2 IN ('gsc') OR SPLIT_PART(campaign,'_',3) = 'homebell' THEN 'brand'
	 	  	WHEN position_2 IN ('gdn') THEN SPLIT_PART(campaign,'_',4)
		  	WHEN tagging_system = 'utm-builder' OR position_2 in ('sn') THEN SPLIT_PART(campaign,'_',3)    
		  	WHEN is_home_landing AND refr_medium = 'search' THEN 'brand'
		  	WHEN tagging_system is null THEN SPLIT_PART(campaign,'_',3)	
		  	ELSE 'na'
		  END AS product_raw,
		  CASE 
		  	WHEN is_lead_provider THEN 'na'
		  	WHEN position_2 = 'sn' THEN SPLIT_PART(sn_camp,'_',4)
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(SPLIT_PART(campaign, '_', 4),'-',1)
		  END AS push,

		  COALESCE(
		  	NULLIF(
		  CASE 
		  	WHEN is_lead_provider THEN 'na'

		  	--WHEN campaign like 'gaw%' AND city_style = 'city' AND SPLIT_PART(campaign,'_',4) LIKE 'city-%' THEN SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',2)
		  	--WHEN campaign like 'gaw%' AND city_style = 'city' THEN SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',3)
		  	<% event_types.forEach(function(col){%>
		  	WHEN campaign like 'gaw%' AND city_style = 'city' AND campaign like '%<%=col%>%' THEN '<%=col%>'

		  		<%})%>
		  	--WHEN position_2 = 'sn' AND city_style = 'city' THEN SPLIT_PART(sn_camp, '_', 5)
		  	--WHEN city_style != 'extended' AND SPLIT_PART(campaign,'_',4) LIKE 'city-%' THEN  SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',2)
		  	--WHEN city_style = 'extended' THEN 'city-extended'
		  END,
		  ''),
		  'na') AS city,

		  COALESCE(NULLIF(CASE 
		  	WHEN is_lead_provider OR medium = 'crm' THEN 'na'
		  	WHEN SPLIT_PART(campaign,'_',1) = 'gaw' AND SPLIT_PART(campaign,'_',5) != 'search' THEN SPLIT_PART(campaign,'_',5)
		  	WHEN city_style = 'extended' THEN COALESCE(NULLIF(SPLIT_PART(term,'_',4),''),NULLIF(SPLIT_PART(sn_camp, '_', 6),''),'na')
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(term, '_', 4)
		  END,''),'na') AS extended_city,
		  CASE
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(campaign, '_', 5) 
		 	ELSE 'na' 
		  END AS audience,
		  CASE 
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(term,'_',1)
		 	ELSE 'na' 
		  END AS sub_audience,
		  CASE 
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(term,'_',2)
		 	ELSE 'na' 
		  END AS sub_campaign,
		  CASE 
		  	WHEN tagging_system = 'utm-builder' THEN SPLIT_PART(term,'_',3)
		 	ELSE 'na' 
		  END AS campaign_date,		  
		  CASE WHEN SPLIT_PART(campaign,'_',2) = 'crm'
		  	THEN SPLIT_PART(campaign,'_',3)||'_'||SPLIT_PART(campaign,'_',4) 
		  END AS crm_data
	  FROM <%=sourceTable %> p
	)
	SELECT 
		utm_id,

(CASE 	

			WHEN (u.source = 'email' OR refr_medium = 'email' or prefix = 'crm') AND medium = 'transactional' AND audience = 'partner' THEN 'transactional partner email'
			WHEN (u.source = 'email' OR refr_medium = 'email' or prefix = 'crm') AND medium = 'transactional' THEN 'transactional customer email'
			WHEN ((u.source = 'email' OR refr_medium = 'email' or prefix = 'crm') AND medium = 'marketing') OR medium = 'email' OR medium = 'crm' OR campaign LIKE '%crm_%' THEN 'marketing email'
			WHEN u.source = 'email' THEN 'email'
            WHEN ((prefix = 'dis' OR medium = 'dis') AND u.source = 'google' AND (audience = 'remarketing' OR campaign_type = 'remarketing')) OR ((prefix = 'dis' OR medium = 'dis') AND u.source = 'display network' AND (audience = 'remarketing' OR campaign_type = 'remarketing'))
			THEN 'display remarketing google'
			WHEN ((prefix = 'dis' OR medium = 'dis') AND u.source in ('google' ,'display network') AND (campaign_type like 'acq%' OR audience like 'acq%')) 
			THEN 'display acquisition google'
			WHEN ((prefix = 'dis' OR medium = 'dis') AND u.source in ('google' ,'display network') AND (audience like 'smart%')) 
			THEN 'display smart google'
			
			WHEN product_raw = 'brand' AND u.source = 'google' and medium = 'cpc' THEN 'paid brand google'
			WHEN product_raw = 'brand' AND u.source = 'bing' and medium = 'cpc' THEN 'paid brand bing'
			WHEN prefix IN ('gaw','sn','gsc','gmb') OR (medium = 'cpc' AND source = 'google') THEN 'paid search google'
		  	WHEN prefix = 'ba' or ( medium = 'cpc' and source = 'bing') THEN 'paid search bing'
		  	WHEN u.source in ('yahoo','de.yahoo.com') or prefix = 'yh' THEN 'yahoo'
		  	/*WHEN refr_medium = 'search' AND medium = 'cpc' THEN 'paid search'*/
		  	
			WHEN  u.source IN ('taboola','trc.taboola.com') or prefix = 'tb'
			THEN 'taboola'
			WHEN  u.source = 'ligatus' or prefix = 'lig'
			THEN 'ligatus'
			
			WHEN (u.source = 'smt' OR prefix = 'smt') AND (audience = 'acquisition' OR campaign_type = 'acquisition' OR audience = 'acqusition' OR audience = 'acqusition')  THEN 'display acquisition sociomantic'
			WHEN (u.source = 'smt' OR prefix = 'smt') AND (audience like 'retarget%' OR campaign_type like 'retarget%') THEN 'display retargeting sociomantic'
			WHEN prefix = 'fb' THEN 'social paid fcbk'
			WHEN medium = 'sp' OR prefix = 'sp' OR medium = 'cpm' THEN 'social paid fcbk'
			WHEN medium = 'so' OR prefix = 'so' OR refr_medium = 'social' THEN 'social organic fcbk'
			
			
			WHEN is_home_landing AND refr_medium = 'search' THEN 'organic brand'
			WHEN refr_medium = 'search' AND campaign !='' THEN 'uncaught search'
			WHEN refr_medium = 'search' THEN 'organic search'
			WHEN medium = 'crm' AND source = 'sms' THEN 'sms'
			WHEN source = 'sms' THEN 'sms'
			WHEN is_home_landing THEN 'direct'
			WHEN COALESCE (medium,campaign, '') != '' THEN 'uncaught utms'

           
			ELSE 'referrer'
		END)::VARCHAR(32) AS mkt_subchannel,

		(CASE 
			
		  	WHEN  mkt_subchannel in ('direct','paid brand google','paid brand bing','organic brand')  THEN 'brand'
		  	WHEN  mkt_subchannel in ('paid search','paid search bing','paid search google') THEN 'paid search'
		  	
			WHEN mkt_subchannel in ('taboola','yahoo','ligatus') THEN 'native'
			
			WHEN mkt_subchannel like 'social paid fcbk' THEN 'social paid'
			WHEN mkt_subchannel like 'social organic fcbk' THEN 'social organic'
			WHEN  mkt_subchannel in ('transactional partner email','transactional customer email','marketing email','email','sms') THEN 'crm'
			--Only google brand campaigns are assigned to brand

			WHEN mkt_subchannel in ('display retargeting sociomantic','display remarketing google') THEN 'display remarketing'
			WHEN mkt_subchannel in ('display acquisition sociomantic','display acquisition google') THEN 'display acquisition'
			
			WHEN mkt_subchannel like 'organic search' THEN 'organic search'

			WHEN mkt_subchannel like 'inbound' THEN 'inbound call'

			WHEN mkt_subchannel like 'referrer' THEN 'referrer'

			WHEN mkt_subchannel in ('internal','uncaught utms') THEN 'unknown'

else 'unknown'
			
		END)::VARCHAR(22) AS mkt_channel,

		(CASE 
			WHEN product_raw = 'brand' THEN 'brand'
			WHEN is_home_landing AND refr_medium = 'search' THEN 'brand'
			WHEN prefix IN ('gaw','sn') OR (medium = 'cpc' AND source = 'google') THEN 'adwords'
		  	WHEN refr_medium = 'search' THEN 'search'
			WHEN medium ='dis' OR u.source IN ('taboola','trc.taboola.com','ligatus','display network')
			THEN 'display'
			WHEN medium = 'ret' OR SPLIT_PART(u.campaign,'_',5) = 'retargeting' OR u.source = 'smt' THEN 'retargeting'
			WHEN medium = 'sp' THEN 'social paid'
			WHEN medium = 'so' OR refr_medium = 'social' THEN 'social organic'
			WHEN u.source = 'mail' OR refr_medium = 'email' THEN 'email'
			ELSE 'referrer'
		END)::VARCHAR(16) AS channel,
		
		
		'web'::VARCHAR(16) AS mkt_medium,
		DECODE(u.product_raw,
  			'painter','painting',
  			'panting','painting',
  			'tiles','tiling',
  			'drywalling','dry_walling',
  			'laminat','laminate',
  			'floor','flooring',
  			'floori','flooring',
  			'parkett','parquet',
  			'fliesen','tiling',
  			u.product_raw
  		) AS product,
	  	f_TO_INT(product,4) AS vertical_type_id,
		medium::VARCHAR(32),
		COALESCE(u.source,'other')::VARCHAR(128) AS source,
		u.source::VARCHAR(128) AS detailed_source,
		u.refr_host::VARCHAR(128) AS referrer,
		campaign::VARCHAR(128),
		term::VARCHAR(128),
		content::VARCHAR(500),
		refr_medium,
		is_lead_provider,
		(is_lead_provider = FALSE)::INT AS bin_internal,
		city_style,
		city,
		extended_city,
		prefix,
		push,
		audience,
		sub_audience,
		campaign_date,
		crm_data,
		--clickid::VARCHAR(256),
		join_id,
		account_id,
  		campaign_id::VARCHAR(32),	
		ad_group_id::VARCHAR(32),
		match_type,
		u.collector_tstamp AS updated_at,
		NULL::bigint AS pbi_utm_id
	FROM prep u 
	--LEFT JOIN refined.top_50_sources tf ON u.source = tf.source
	WHERE u.ranking = 1
);
<%if(recreate) {%>
INSERT INTO dims.channels
SELECT 
	MD5('') AS utm_id,
	'direct' AS channel,
	'brand' AS mkt_channel,
	'direct' AS mkt_subchannel,
	'web' AS mkt_medium,
	'na' AS product,
	0 AS vertical_type_id,
	'direct' AS medium,
	'direct' AS source,
	'direct' AS detailed_source,
	'' AS referrer,
	'' AS campaign,
	'' AS term,
	'' AS content,
	'' AS refr_medium,
	FALSE AS is_lead_provider,
	1 AS bin_internal,
	'na' AS city_style,
	'na' AS city,
	'na' AS extended_city,
	'na' AS prefix,
	'na' AS push,
	'na' AS audience,
	'na' AS sub_audience,
	'na' AS campaign_date,
	'na' AS crm_data,
		--clickid::VARCHAR(256),
	'na' AS join_id,
	'na' AS account_id,
  	'na' AS campaign_id,	
	'na' AS ad_group_id,
	'na' AS match_type,
	'1970-01-01'::timestamp AS updated_at,
	NULL::bigint AS pbi_utm_id
UNION ALL
SELECT 
	MD5('na') AS utm_id,
	'untracked' AS channel,
	'untracked' AS mkt_channel,
	'untracked' AS mkt_subchannel,
	'untracked' AS mkt_medium,
	'na' AS product,
	0 AS vertical_type_id,
	'untracked' AS medium,
	'untracked' AS source,
	'untracked' AS detailed_source,
	'na' AS referrer,
	'na' AS campaign,
	'na' AS term,
	'na' AS content,
	'' AS refr_medium,
	FALSE AS is_lead_provider,
	1 AS bin_internal,
	'na' AS city_style,
	'na' AS city,
	'na' AS extended_city,
	'na' AS prefix,
	'na' AS push,
	'na' AS audience,
	'na' AS sub_audience,
	'na' AS campaign_date,
	'na' AS crm_data,
		--clickid::VARCHAR(256),
	'na' AS join_id,
	'na' AS account_id,
  	'na' AS campaign_id,	
	'na' AS ad_group_id,
	'na' AS match_type,
	'1970-01-01'::timestamp AS updated_at,
	NULL::bigint AS pbi_utm_id
UNION ALL
	SELECT 
	MD5('timeout') AS utm_id,
	'timeout' AS channel,
	'timeout' AS mkt_channel,
	'timeout' AS mkt_subchannel,
	'timeout' AS mkt_medium,
	'na' AS product,
	0 AS vertical_type_id,
	'timeout' AS medium,
	'timeout' AS source,
	'timeout' AS detailed_source,
	'na' AS referrer,
	'na' AS campaign,
	'na' AS term,
	'na' AS content,
	'' AS refr_medium,
	FALSE AS is_lead_provider,
	1 AS bin_internal,
	'na' AS city_style,
	'na' AS city,
	'na' AS extended_city,
	'na' AS prefix,
	'na' AS push,
	'na' AS audience,
	'na' AS sub_audience,
	'na' AS campaign_date,
	'na' AS crm_data,
		--clickid::VARCHAR(256),
	'na' AS join_id,
	'na' AS account_id,
  	'na' AS campaign_id,	
	'na' AS ad_group_id,
	'na' AS match_type,
	'1970-01-01'::timestamp AS updated_at,
	NULL::bigint AS pbi_utm_id

UNION ALL
SELECT
	MD5('loading') AS utm_id,
	'loading' AS channel,
	'loading' AS mkt_channel,
	'loading' AS mkt_subchannel,
	'loading' AS mkt_medium,
	'loading' AS product,
	0 vertical_type_id,
	'loading' AS medium,
	'loading' AS source,
	'loading' AS detailed_source,
	'loading' AS referrer,
	'loading' AS campaign,
	'loading' AS term,
	'loading' AS content,
	'loading' AS refr_medium,
	FALSE AS is_lead_provider,
	0 bin_internal,
	'loading' AS city_style,
	'loading' AS city,
	'loading' AS extended_city,
	'loading' AS prefix,
	'loading' AS push,
	'loading' AS audience,
	'loading' AS sub_audience,
	'loading' AS campaign_date,
	'loading' AS crm_data,
		--clickid::VARCHAR(256),
	'loading' AS join_id,
	'loading' AS account_id,
	'loading' AS campaign_id,	
	'loading' AS ad_group_id,
	'na' AS match_type,
	'1970-01-01'::timestamp AS updated_at,
	1::bigint AS pbi_utm_id
<%}%> 



