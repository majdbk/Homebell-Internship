
<% 
	var recreate = false;
%>
<%if(recreate){%>

DROP TABLE IF EXISTS dims.leads;
CREATE TABLE dims.leads
SORTKEY(lead_created_at)
DISTKEY(lead_id)
AS (
WITH
<%}else{%>
DROP TABLE IF EXISTS temp_leads;
CREATE TEMPORARY TABLE temp_leads AS (
	
	WITH leads_from_opps AS (
		SELECT lead_id::int 
		FROM dims.leads
		WHERE sf_lead_id IN (SELECT legacy_lead_id 
 			FROM raw_data.sf_opportunities
			WHERE last_modified_date > (SELECT MAX(etl_at)-'3 hours'::interval FROM dims.leads))
	),

	leads_from_conversions AS (
		SELECT tr_orderid::int 
		FROM refined.onsite_conversions
		WHERE collector_tstamp > (SELECT MAX(etl_at)-'3 hours'::interval FROM dims.leads)
	),

	new_leads AS (
		SELECT backend_opportunity_id::int 
		FROM raw_data.sf_leads
		WHERE last_modified_date > (SELECT MAX(etl_at)-'3 hours'::interval FROM dims.leads)
	),
	changed_leads AS (
		SELECT * FROM leads_from_opps 
		UNION ALL
		SELECT * FROM leads_from_conversions 
		UNION ALL
		SELECT * FROM new_leads
	),
<%}%>
	
	agents AS (
		SELECT lead_id, 
			agent_id,
			lead_touchpoint_did,
			ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_touchpoint_at DESC) AS ranking_last_agent,
			ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_touchpoint_at ASC) AS ranking_first_agent,
			MIN(lead_touchpoint_at) OVER (PARTITION BY lead_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touchpoint_at,
			MAX(lead_touchpoint_at) OVER (PARTITION BY lead_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_touchpoint_at
			FROM scratch.agents_leads_mapping
		WHERE agent_id != '00G58000000ItwVEAS' --Ignore the lead queue
		<%if(!recreate){%>
		AND lead_id IN (
			SELECT * FROM changed_leads
		) 
		<%}%>
	),
  	prep AS (
		SELECT
			$lead_id = COALESCE(mp.hb_lead_id,l.backend_opportunity_id::INT); $lead_id AS lead_id,
			l.id as sf_lead_id,
			$lead_id AS lead_conversion_id,
			mp.sf_opportunity_id,

			l.owner_id as agent_id,
			COALESCE(fa.agent_id,l.owner_id) as first_agent_id,
			COALESCE(la.agent_id,l.owner_id) as last_agent_id,
			
			l.created_date AS lead_created_at,
			fa.first_touchpoint_at,
			fa.last_touchpoint_at,
			$is_internal = (COALESCE(LOWER(l.source),'') IN ('homebell - intern', 'homebell', 'homebell -intern','referral (intern)'))::SMALLINT; $is_internal AS is_internal_lead,
			CASE WHEN $is_internal THEN 'internal' ELSE 'external' END AS sourcing_type, 
			CASE 
				WHEN LOWER(l.source) LIKE 'homebe%' OR l.source = 'Referral (intern)'
				THEN 'homebell' 
				ELSE COALESCE(LOWER(l.source),'') 
			END AS lead_source,
			DECODE(LOWER(lead_campaign),'n/a','na','','na',COALESCE(LOWER(lead_campaign),'')) AS lead_campaign,
			COALESCE(LOWER(l.mkt_source),'') AS mkt_source,
			COALESCE(LOWER(l.mkt_medium),'') AS mkt_medium,
			COALESCE(LOWER(l.mkt_campaign),'') AS mkt_campaign,
			l.score as lead_score,
			l.price as lead_price,
			LOWER(COALESCE(l.source_id,'')) as lead_source_id,
			CASE WHEN salutation = 'Mr.' THEN 'M' WHEN salutation = 'Mrs.' THEN 'F' ELSE NULL END AS gender_customer, 
		    l.postal_code as lead_postal_code,
		    TRIM(LOWER(COALESCE(cm.opportunities_country,l.job_country,'')||COALESCE(l.job_city,l.city,''))) as geo_id,
		    LOWER(COALESCE(cm.opportunities_country,l.job_country,''))||f_STRIP_SPECIAL(COALESCE(l.postal_code,l.job_postal_code,'')) as zip_id,
		    l.city as lead_city,
		    cm.opportunities_country AS country,

		    l.duplicate_of as duplicate_of,
		    CASE 
		    	WHEN l.status LIKE 'Dublicate' THEN 'duplicate'
		    	WHEN l.status LIKE 'Untouched' OR LOWER(l.status) LIKE 'not contacted' THEN 'new'
		    	WHEN LOWER(l.status) = 'not interested' THEN 'unqualified'
		    	WHEN l.status LIKE '___ Call%' THEN 'in progress'
		    	ELSE LOWER(l.status)
		    END as status,
		    l.status AS raw_status,
		    l.substatus,
		    
		   
		    CASE WHEN l.substatus IN ('Duplicate','Fake request') OR l.substatus LIKE 'Wrong data%' 
		    	THEN FALSE
		    	ELSE TRUE
		    END as is_valid_lead,
		    COALESCE(l.substatus IN ('Duplicate','Fake request','Expired in lead queue') OR l.substatus LIKE 'Wrong data%',TRUE)::SMALLINT
		    AS is_reached_lead,
			l.status IN ('Qualified','Unqualified') AS is_closed_lead,
		    l.status = 'Qualified' AS is_qualified,
		    $has_opportunity = (mp.sf_offer_id IS NOT NULL)::SMALLINT;  $has_opportunity AS has_opportunity,
		   	 $has_opportunity AS is_converted_lead,
		   	l.unqualified_reason,
		    LOWER(l.main_vertical) as lead_main_vertical,
			<%[1,2,3,4,5,6,7].forEach(function(n){%>
			l.call_<%=n%>_timestamp,
			<%});%>																					 
		    l.lead_process_end,
		    COUNT(l.id) OVER (PARTITION BY l.id) as associated_opportunities_count,
		    l.last_modified_date,
		    CASE WHEN l.record_type_id = '01258000000VNacAAG' THEN 'Partner Lead'
		         WHEN l.record_type_id = '01258000000VCHvAAO' THEN 'Lead'
		         WHEN l.record_type_id = '01258000000VCHuAAO' THEN 'Existing Customer'
		    END AS record_type,
		    ls.created_at AS lead_score_created_at,
		    ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY mp.qualified_at ASC) as sf_ranking,
		    ROW_NUMBER() OVER (PARTITION BY $lead_id ORDER BY mp.qualified_at ASC) as hb_ranking
		FROM raw_data.sf_leads l
		LEFT JOIN scratch.opportunities_offers_map mp on mp.sf_lead_id = l.id
		LEFT JOIN raw_data.hb_lead_scores ls ON ls.opportunity_id = mp.hb_lead_id
		LEFT JOIN scratch.country_mapping AS cm ON cm.leads_country=l.country
		LEFT JOIN agents AS fa ON fa.lead_id = $lead_id AND fa.ranking_first_agent = 1
		LEFT JOIN agents AS la ON la.lead_id = $lead_id AND la.ranking_last_agent = 1
		WHERE l.is_trainings_lead = FALSE 
		AND (l.record_type_id != '01258000000VNacAAG' OR l.created_date < '2016-10-01') -- partner lead
		<%if(!recreate){%>
		AND l.backend_opportunity_id::INT IN (
			SELECT * FROM changed_leads
		) 
		<%}%>
	)
	SELECT DISTINCT 
		MD5(
            COALESCE(p.lead_source,'') ||
            COALESCE(p.lead_campaign,'') ||
            COALESCE(o.mkt_campaign,p.mkt_campaign,'')||
            COALESCE(o.mkt_source,p.mkt_source,'')||
            COALESCE(o.mkt_medium,p.mkt_medium,'')||
            f_TO_INT(p.lead_main_vertical,4)::VARCHAR
        ) AS utm_id,
		p.lead_id,
		p.sf_lead_id,
		p.lead_conversion_id,
		COALESCE(o.sales_agent_lookup,p.agent_id) as agent_id,
		COALESCE(p.first_agent_id,o.sales_agent_lookup) as first_agent_id,
		COALESCE(o.sales_agent_lookup,p.last_agent_id) as last_agent_id,

		lead_created_at,
		first_touchpoint_at,
		last_touchpoint_at,
		f_TO_DID(lead_created_at)::INT AS lead_created_did,
		o.created_date AS qualified_at,
		f_TO_DID( qualified_at)::INT AS qualified_did,
		COALESCE(o.created_date,CASE WHEN p.is_closed_lead = TRUE THEN p.last_modified_date ELSE NULL END) AS lead_closed_at, 
		f_TO_DID(COALESCE(o.created_date,CASE WHEN p.is_closed_lead = TRUE THEN p.last_modified_date ELSE NULL END))::INT AS lead_closed_did,
		lead_process_end AS lead_process_end_at,
		f_TO_DID( lead_process_end)::INT AS lead_process_end_did,

		LOWER(COALESCE(p.lead_source,'')) as lead_source,
		LOWER(COALESCE(p.lead_campaign,'')) as lead_campaign,
		LOWER(COALESCE(p.mkt_source,'')) AS mkt_source,
		LOWER(COALESCE(p.mkt_medium,'')) AS mkt_medium,
		LOWER(COALESCE(p.mkt_campaign,'')) as mkt_campaign,
		COALESCE(rs.landing_pagepath,lp.landing_page) as landing_page,
		lp.is_spinfire,
		p.lead_score,
		p.lead_price,
		p.lead_source_id,
		p.gender_customer,
		LEFT(p.geo_id,48)::VARCHAR(48) AS geo_id,
		LEFT(p.zip_id,48)::VARCHAR(48) AS zip_id,
		COALESCE(o.job_country,p.country) AS country,
		p.lead_city,
		p.lead_postal_code,

		c.session_id,
		c.conversion_type,
		c.conversion_path_id,
		c.onsite_conversion_locale_tstamp,
		c.domain_userid,
		c.onsite_user_id,

		p.duplicate_of,
		p.status,
		p.raw_status,
		p.substatus,
		$is_valid_lead =  CASE WHEN p.is_valid_lead = FALSE OR LOWER(o.opscancellationreason) LIKE 'fake booking%' THEN FALSE ELSE TRUE END;
		$is_valid_lead AS is_valid_lead,
		($is_valid_lead)::INT bin_valid_lead,
		$is_reached_lead = CASE WHEN p.is_reached_lead = FALSE OR LOWER(o.opscancellationreason) LIKE 'fake booking%' THEN FALSE ELSE TRUE END;
		$is_reached_lead AS is_reached_lead,
		($is_reached_lead)::INT bin_reached_lead,
		p.is_closed_lead,
		p.is_qualified,
		p.has_opportunity,
		p.is_converted_lead,
		p.unqualified_reason,
		p.is_internal_lead,
		p.is_internal_lead::INT AS bin_internal_lead,
		p.is_closed_lead::INT AS bin_closed_lead,


		p.sourcing_type,

		p.lead_main_vertical,
		f_TO_INT(p.lead_main_vertical,4) AS primary_vertical_type_id,
	    <%[1,2,3,4,5,6,7].forEach(function(n){%>
		p.call_<%=n%>_timestamp,
		<%});%>	
	    CURRENT_TIMESTAMP AS etl_at,
	    p.associated_opportunities_count,
	    p.record_type,
	    p.lead_score_created_at
	FROM prep p 
	LEFT JOIN raw_data.sf_opportunities o ON o.id = p.sf_opportunity_id
	LEFT JOIN refined.onsite_conversions c ON c.tr_orderid = p.lead_conversion_id
	LEFT JOIN scratch.landing_metadata lp ON lp.campaign = LOWER(p.lead_campaign)
	LEFT JOIN refined.sessions rs ON rs.session_id = c.session_id
	WHERE p.sf_ranking = 1 AND p.hb_ranking = 1 AND lead_id IS NOT NULL
);
<%if(!recreate){%>
DELETE FROM dims.leads
WHERE lead_id IN (SELECT lead_id FROM temp_leads);

INSERT INTO dims.leads
(SELECT * FROM temp_leads);
<%}%>



