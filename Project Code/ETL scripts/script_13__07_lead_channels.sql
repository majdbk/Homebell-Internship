

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
%>
INSERT INTO dims.channels
(	utm_id, channel,mkt_subchannel,mkt_channel,mkt_medium,product,vertical_type_id,
	medium,source,detailed_source,referrer,campaign,
	term,content,refr_medium,is_lead_provider,bin_internal,
	city_style,city,extended_city,prefix,
	push,audience,sub_audience,
	campaign_date,
	--crm_data,
	--join_id,account_id,campaign_id,ad_group_id,
	updated_at
)
WITH prep AS (
	SELECT 
		utm_id,
		CASE 
		WHEN mkt_source != '' THEN mkt_source
		WHEN mkt_campaign LIKE 'gsc%' OR mkt_campaign LIKE 'gaw%' OR mkt_campaign LIKE 'gmb%' THEN 'google'
		WHEN mkt_campaign LIKE 'ba%' THEN 'bing'
		WHEN mkt_campaign LIKE 'sp%' THEN 'social paid'
		WHEN mkt_campaign LIKE 'so%' THEN 'social organic'
		WHEN mkt_campaign LIKE 'crm%' THEN 'crm'
		ELSE lead_source END AS source,

    CASE WHEN mkt_campaign LIKE 'gsc%'  THEN 'brand'

		WHEN mkt_campaign LIKE 'sn%' OR mkt_campaign LIKE 'gaw%' OR mkt_campaign LIKE 'ba%' OR mkt_campaign LIKE 'gmb%'  THEN 'paid search'

		 WHEN mkt_source in ('taboola','ligatus','yahoo') THEN 'native'

		 	WHEN mkt_source LIKE 'smt%' THEN 'retargeting'



		WHEN mkt_campaign LIKE 'sp%' or mkt_campaign LIKE 'fb%' THEN 'social paid'


		WHEN mkt_campaign LIKE 'so%' THEN 'social organic'


		WHEN mkt_campaign LIKE 'crm%' THEN 'crm'

	    WHEN lead_source = 'homebell' THEN 'internal'

		WHEN lead_source = 'inbound call' THEN 'inbound' 

		ELSE 'lead provider'
		END as channel,

		lead_source IN ('homebell','inbound call') AS is_internal,
		COALESCE(NULLIF(mkt_medium,''),lead_campaign) AS medium,
		mkt_campaign AS campaign,
		primary_vertical_type_id AS vertical_type_id,
		lead_main_vertical AS product,
		<%[1,2,3,4,5].forEach(function(p){ %>
		SPLIT_PART(campaign,'_',<%=p%>) AS camp_<%=p%>,
		<%})%>
		SPLIT_PART(campaign,'_',2) IN ('de','nl','ac','ch') AS is_new,
		LAST_VALUE(lead_created_at) OVER 
  			(PARTITION BY utm_id 
			 ORDER BY lead_created_at ASC 
			 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		) AS updated_at,
		ROW_NUMBER() OVER (PARTITION BY utm_id ORDER BY lead_created_at ASC) AS ranking
	FROM dims.leads

	WHERE 
	<% if(!recreate) {%>
	lead_created_at > (CURRENT_DATE - 90) AND
	<% }%>
	utm_id NOT IN (SELECT utm_id  FROM dims.channels)
)
SELECT 
	utm_id,
	(channel)::VARCHAR(24) AS channel,
	

	( CASE WHEN campaign LIKE 'gsc%' and u.source like 'bing'  THEN 'paid brand bing'
	 WHEN campaign like 'gsc%' and u.source ='google'  THEN 'paid brand google'
	
		 WHEN campaign like 'gsc%' and u.source = 'facebook'  THEN 'paid brand fcbk'
 WHEN campaign like 'dis%' AND SPLIT_PART(campaign,'_',4) = 'remarketing'
			THEN 'display remarketing google'

			 WHEN campaign like 'dis%' AND SPLIT_PART(campaign,'_',4) = 'acquisition' 
			THEN 'display acquisition google'

		 WHEN campaign like 'sn%' OR campaign like 'gaw%' OR campaign like 'ba%' OR campaign like 'gmb%'  THEN 'paid search google'

 WHEN campaign like 'ba%' THEN 'paid search bing'

		 

 WHEN u.source = 'taboola' THEN 'taboola'
 WHEN u.source = 'ligatus' THEN 'ligatus'
 WHEN u.source = 'yahoo' THEN 'yahoo'


		
            WHEN (u.source = 'smt' OR campaign like 'smt%') AND SPLIT_PART(campaign,'_',4) like 'retarget%' THEN 'display retargeting sociomantic'
			WHEN (u.source = 'smt' OR campaign like 'smt%') AND SPLIT_PART(campaign,'_',4) = 'acquisition' THEN 'display acquisition sociomantic'


		WHEN campaign like 'sp%' THEN 'social paid fcbk'

		WHEN  campaign like'fb%' THEN 'social paid fcbk'


		WHEN campaign like'so%' THEN 'social organic fcbk'


		
		WHEN (campaign like 'crm%') AND medium = 'transactional' AND SPLIT_PART(campaign,'_',5) = 'partner' THEN 'transactional partner email'
		WHEN (campaign like 'crm%') AND medium = 'transactional'  THEN 'transactional customer email'
		WHEN (campaign like 'crm%')   THEN 'marketing email'
	    

		WHEN u.source = 'inbound call' THEN 'inbound' 

		When u.source in ('skydreams','stucadooradviseur','vakmanvinden','watkostdeschilder','watkosteenstucadoor','watkosteenvloer','daa','maler4me') THEN 'lead provider'

          WHEN u.source like 'homebell' THEN 'internal'


		ELSE 'uncaught utms'
		END)::VARCHAR(32) AS mkt_subchannel ,




		(CASE 
			
		  	WHEN  mkt_subchannel in ('direct','paid brand google','paid brand bing','organic brand')  THEN 'brand'
		  	WHEN  mkt_subchannel in ('paid search','paid search bing','paid search google') THEN 'paid search'
		  	WHEN mkt_subchannel in ('display retargeting sociomantic','display remarketing google') THEN 'display remarketing'
			WHEN mkt_subchannel in ('taboola','yahoo','ligatus') THEN 'native'
			WHEN mkt_subchannel in ('display acquisition sociomantic','display acquisition google') THEN 'display acquisition'
			WHEN mkt_subchannel like 'social paid fcbk' THEN 'social paid'
			WHEN mkt_subchannel like 'social organic fcbk' THEN 'social organic'
			WHEN  mkt_subchannel in ('transactional partner email','transactional customer email','marketing email','email','sms') THEN 'crm'
			--Only google brand campaigns are assigned to brand
			
			WHEN mkt_subchannel like 'organic search' THEN 'organic search'

			WHEN mkt_subchannel like 'inbound' THEN 'inbound call'

			WHEN mkt_subchannel like 'referrer' THEN 'referrer'

			

			WHEN mkt_subchannel like 'lead provider' THEN 'lead provider'

else  'unknown'
			
		END)::VARCHAR(22) AS mkt_channel,


	CASE 
		WHEN channel IN ('adwords','bing','social paid','social organic','crm','internal') THEN 'web'
		WHEN channel IN ('lead provider','inbound') THEN 'external'
		WHEN channel = 'tv' THEN 'tv' 
		ELSE 'web' 
	END AS mkt_medium,
	u.product,
  	u.vertical_type_id,
	channel::VARCHAR(32) AS medium,
	u.source::VARCHAR(128) AS source,
	u.source::VARCHAR(128) AS detailed_source,
	'untracked'::VARCHAR(128) AS referrer,
	campaign::VARCHAR(128),
	u.medium::VARCHAR(128) AS term,
	NULL::VARCHAR(500) AS content,
	u.medium AS refr_medium,
	channel = 'lead provider' AS is_lead_provider,
	(channel != 'lead provider')::INT AS bin_internal,

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

	
		  CASE 

		  	--WHEN campaign like 'gaw%' AND city_style = 'city' AND SPLIT_PART(campaign,'_',4) LIKE 'city-%' THEN SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',2)
		  	--WHEN campaign like 'gaw%' AND city_style = 'city' THEN SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',3)
		  	<% event_types.forEach(function(col){%>
		  	WHEN campaign like 'gaw%' AND city_style = 'city' AND campaign like '%<%=col%>%' THEN '<%=col%>'

		  		<%})%>
		  	
		  	--WHEN city_style != 'extended' AND SPLIT_PART(campaign,'_',4) LIKE 'city-%' THEN  SPLIT_PART(SPLIT_PART(campaign,'_',4),'-',2)
		  	--WHEN city_style = 'extended' THEN 'city-extended'
		  	ELSE 'na'
		  END AS city,
	'na' AS extended_city,
	CASE 
		WHEN channel = 'lead provider' THEN 'ext' 
		WHEN is_new THEN camp_1
		ELSE camp_2	
	END AS prefix,
	CASE 
		WHEN is_new AND camp_4 LIKE 'city%' THEN 'city'
		WHEN is_new THEN SPLIT_PART(camp_4,'-',1)
		ELSE camp_3
	END AS push,
	CASE 
		WHEN is_new THEN camp_5 
		ELSE 'na'
	END audience,
	'na' sub_audience,
	'na' campaign_date,
	u.updated_at
FROM prep u 
WHERE ranking = 1
