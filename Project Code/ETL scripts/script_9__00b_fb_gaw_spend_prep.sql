

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


DROP TABLE IF EXISTS scratch.mkt_spend;
CREATE TABLE scratch.mkt_spend AS
(
	WITH fb_prep AS (
		SELECT 
			CASE 
			  WHEN LOWER(account_name) LIKE 'homebell_de'
			  THEN 'de'
			  ELSE 'na'
			END AS geo_id,
			'social' AS refr_medium,
			LOWER(account_id)::VARCHAR(255) as account_id,
			campaign_id,
			adset_id AS ad_group_id,
			'facebook'::VARCHAR(32) as source, 
			'sp'::VARCHAR(24) as medium,
			LOWER(campaign_name)::VARCHAR(255) as campaign,
			LOWER(adset_name)::varchar as term,
			'na' as match_type,
			DATE(date) as date,
			sum(impressions)::int AS impressions,
			sum(spend)::float AS cost,
			sum(clicks)::int AS clicks,
			sum(reach)::int AS reach
		FROM raw_data.fb_adset_impressions
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11
	),
	gaw_prep AS (
		SELECT
			CASE 
				WHEN LOWER(account_name) LIKE 'de%'
				OR   LOWER(account_name) LIKE 'germ%'
				THEN 'de'
				WHEN LOWER(account_name) LIKE 'nl%'
				OR   LOWER(account_name) LIKE 'neth%'
				THEN 'nl'
				ELSE 'na'
			END
			AS geo_id,
			'search' AS refr_medium,
            LOWER(r.account_id)::VARCHAR(255) AS account_id,
			r.campaign_id,
			r.ad_group_id,
			DECODE(LOWER(r.ad_network_type1),'search network','google',LOWER(r.ad_network_type1)) AS source,
			COALESCE(mp.medium,'cpc')::VARCHAR(255) as medium,
			LOWER(COALESCE(mp.campaign,r.campaign_name))::VARCHAR(255) AS campaign,
			LOWER(COALESCE(mp.term,r.ad_group_name))::varchar as term,
			f_LOWER_NOT_NULL(SPLIT_PART(mp.term,'_',1)) match_type,
			DATE(date) as date,
			sum(impressions)::int as impressions,
			sum(cost)::FLOAT as spend,
			sum(clicks)::int as clicks,
			null::int as reach
		FROM raw_data.gaw_adgroup_report r
		LEFT JOIN scratch.adgroup_utm_map mp ON mp.join_id = r.account_id||r.campaign_id||r.ad_group_id
		--WHERE date >= CURRENT_DATE - 7 AND date IS NOT NULL
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11
	)
	SELECT 
		MD5(source||source||medium||campaign||term||''||'f') AS utm_id,
		'utm-builder' AS tagging_system,
		SPLIT_PART(campaign,'_',2) AS position_2,
		false AS is_lead_provider,
  		false AS is_home_landing,
		CASE WHEN SPLIT_PART(campaign,'_',5) = 'extended' 
			THEN 'extended'
			WHEN SPLIT_PART(campaign,'_',4) LIKE 'city-extended%' 
			THEN 'extended'
			WHEN campaign like '%extended%' and campaign like 'gaw%'
		    THEN 'extended'

		     <% event_types.forEach(function(col){%> 
		    WHEN (campaign LIKE '%city%' and campaign like 'gaw%') OR (campaign like 'gaw%' and campaign like '%<%=col%>%')or (campaign like 'gaw%' and campaign like '%<%=col%>%' and campaign LIKE '%city%')
			THEN 'city'
			<%})%>
			<% event_types.forEach(function(col){%> 
		    WHEN campaign like 'gaw%' and (campaign not like '%city%' AND campaign NOT like '%<%=col%>%')
		    THEN 'non_city'
			<%})%>

			ELSE 'na'
		END AS city_style,
		CASE WHEN SPLIT_PART(campaign,'_',2) = 'sn'
			THEN SPLIT_PART(campaign,'$',1) 
		END AS sn_camp,
		null::varchar as adgroup_id_from_utm,
  		date::timestamp AS collector_tstamp,
  		source AS refr_host,
  		'' AS content,
  		'' AS join_id,
  		ROW_NUMBER() OVER (
  			PARTITION BY MD5(source||source||medium||campaign||term||''||'f') 
  			ORDER BY date DESC
  		) AS ranking,
		* 
	FROM (
		SELECT * FROM fb_prep
		UNION ALL
		SELECT * FROM gaw_prep
	)
);
