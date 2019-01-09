
<%
	var recreate = false;

%>
<%if(recreate) {%>
DROP TABLE IF EXISTS dims.geo;
CREATE TABLE dims.geo 
	SORTKEY(geo_id)
	DISTKEY(geo_id)
AS 
<%}else{%> 
INSERT INTO dims.geo
<%}%>
(
	WITH prep AS (
		SELECT
			TRIM(LOWER(COALESCE(cm.opportunities_country,l.job_country,''))) AS country, 
			TRIM(LOWER(COALESCE(l.job_city,l.city,''))) AS city 
		FROM raw_data.sf_leads AS l
		LEFT JOIN scratch.country_mapping AS cm ON cm.leads_country=l.country
		UNION ALL
		SELECT
		 	LOWER(TRIM(COALESCE(job_country,''))) AS country,
		 	LOWER(TRIM(COALESCE(job_city,''))) AS city 
		FROM raw_data.sf_opportunities
		UNION ALL
		SELECT 
			LOWER(TRIM(COALESCE(u.country,cm.opportunities_country,''))) AS country,
			LOWER(TRIM(COALESCE(u.city,sf.billing_city,''))) AS city
		FROM raw_data.ptnr_partner_users u 
		LEFT JOIN raw_data.sf_accounts sf ON sf.partner_backend_id = u.id
		LEFT JOIN scratch.country_mapping AS cm ON cm.leads_country = sf.billing_country
		
		UNION ALL

		SELECT 
		 LOWER(country) AS country,
		 LOWER(city) AS city
		FROM refined.sessions
	),
	geo_ids AS (
		SELECT 
			$geo_id = TRIM(LOWER(COALESCE(country,'')||COALESCE(city,'')));
			$geo_id as geo_id,
			DECODE(country,
				'de','dach',
				'at','dach',
				'ch','dach',
				'nl','benelux',
				'be','benelux',
				'us','usa',
				'other') AS region,
			COALESCE(country,'') AS country,
			COALESCE(city,'') AS city,
			ROW_NUMBER() OVER (PARTITION BY $geo_id ) as ranking
		FROM prep
		<%if(!recreate) {%>
		WHERE $geo_id NOT IN (SELECT geo_id FROM dims.geo)
		<%}%>
	)
	SELECT 
		geo_id,
		region,
		country,
		city,
		(region IN ('dach','benelux'))::INT AS is_active_region 
	FROM geo_ids 
	WHERE ranking = 1
	<%if(recreate) {%>
	UNION ALL
	SELECT 
		'na' geo_id,
		'na' region,
		'na' country,
		'na' city, 
	FALSE AS is_active_region
	<%}%>	
);






