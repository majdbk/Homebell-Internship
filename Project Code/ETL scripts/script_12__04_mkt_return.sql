

DROP TABLE IF EXISTS scratch.online_conversions;
CREATE TABLE scratch.online_conversions 
AS
(
    SELECT 
        sm.lead_id,
        sm.lead_created_at,
        COALESCE(s.start_did,lead_created_did) as date_id,
        COALESCE(s.utm_id,sm.utm_id,MD5('na')) AS utm_id,
        primary_vertical_type_id AS marketed_vertical_id,
        COALESCE(s.session_id,'na') AS session_id,
        COALESCE(s.geo_id,sm.geo_id) as geo_id,
        -- cost when its external
        SUM(sm.lead_price) AS cost,
        -- all the other standard ones
        SUM(COALESCE(cp.custom,1)*sm.leads) AS leads,
        SUM(COALESCE(cp.custom,1)*sm.valid_leads) AS valid_leads,
        --SUM(COALESCE(cp.custom,1)*sm.processed_leads) AS processed_leads,
        SUM(COALESCE(cp.custom,1)*sm.qualified_leads) AS qualified_leads,
        SUM(COALESCE(cp.custom,1)*sm.offers) AS offers,
        --SUM(COALESCE(cp.custom,1)*sm.closes) AS closes,
        SUM(COALESCE(cp.custom,1)*sm.orders) AS orders,
        SUM(COALESCE(cp.custom,1)*sm.booked_number) AS booked_number,
        SUM(COALESCE(cp.custom,1)*sm.completed_number) AS completed_number,
        SUM(COALESCE(cp.custom,1)*sm.potential_nmv) AS potential_nmv,
        SUM(COALESCE(cp.custom,1)*sm.gmv) as gmv,
        SUM(COALESCE(cp.custom,1)*sm.nmv) as nmv,
        SUM(COALESCE(cp.custom,1)*sm.booked_nmv) as booked_nmv,
        SUM(COALESCE(cp.custom,1)*sm.completed_nmv) as completed_nmv,
        SUM(COALESCE(cp.custom,1)*sm.uncancelled_nmv) as uncancelled_nmv,
        SUM(COALESCE(cp.custom,1)*sm.created_margin) as created_margin,
        SUM(COALESCE(cp.custom,1)*sm.completed_margin) as completed_margin,
        SUM(COALESCE(cp.custom,1)*sm.uncancelled_margin) as uncancelled_margin,
        SUM(COALESCE(cp.last,1)*sm.leads) AS lc_leads,
        SUM(COALESCE(cp.last,1)*sm.valid_leads) AS lc_valid_leads,
        SUM(COALESCE(cp.last,1)*sm.qualified_leads) AS lc_qualified_leads,
        SUM(COALESCE(cp.last,1)*sm.offers) AS lc_offers,
        SUM(COALESCE(cp.last,1)*sm.orders) AS lc_orders,
        SUM(COALESCE(cp.last,1)*sm.booked_number) AS lc_booked_number,
        SUM(COALESCE(cp.last,1)*sm.completed_number) AS lc_completed_number,
        SUM(COALESCE(cp.last,1)*sm.potential_nmv) AS lc_potential_nmv,
        SUM(COALESCE(cp.last,1)*sm.gmv) as lc_gmv,
        SUM(COALESCE(cp.last,1)*sm.nmv) as lc_nmv,
        SUM(COALESCE(cp.last,1)*sm.booked_nmv) as lc_booked_nmv,
        SUM(COALESCE(cp.last,1)*sm.completed_nmv) as lc_completed_nmv,
        SUM(COALESCE(cp.last,1)*sm.uncancelled_nmv) as lc_uncancelled_nmv,
        SUM(COALESCE(cp.last,1)*sm.created_margin) as lc_created_margin,
        SUM(COALESCE(cp.last,1)*sm.completed_margin) as lc_completed_margin,
        SUM(COALESCE(cp.last,1)*sm.uncancelled_margin) as lc_uncancelled_margin
    FROM scratch.lead_summary sm  
    LEFT JOIN refined.conversion_path cp ON cp.conversion_path_id = sm.conversion_path_id
    LEFT JOIN refined.sessions s ON cp.session_id = s.session_id 
    GROUP BY 1,2,3,4,5,6,7
);

DELETE FROM facts.marketing
WHERE lead_id IN (SELECT lead_id FROM scratch.online_conversions)
AND 'lead' = fact_subtype;
INSERT INTO facts.marketing
(
    -- basic
    event_at,fact_type,fact_subtype,
    -- forgein keys
    session_id,user_id,lead_id,date_id,session_did,utm_id,
    geo_id,marketed_vertical_id,vertical_type_id,
    -- specific
    leads,valid_leads,
    --processed_leads,
    qualified_leads,offers,
    --closes,
    orders,booked_number,completed_number,potential_nmv,gmv,nmv,booked_nmv,completed_nmv,
    uncancelled_nmv,created_margin,completed_margin,uncancelled_margin,
    lc_leads,lc_valid_leads,lc_qualified_leads,lc_offers,lc_orders,lc_booked_number,lc_completed_number,lc_potential_nmv,lc_gmv,lc_nmv,lc_booked_nmv,lc_completed_nmv,
    lc_uncancelled_nmv,lc_created_margin,lc_completed_margin,lc_uncancelled_margin,

    cost,

    pbi_utm_id
)

(
    SELECT
    --basic
        lead_created_at AS event_at,
        'conversion' AS fact_type,
        'lead' AS fact_subtype,
    --foreign keys
        s.session_id,
        s.lead_id AS user_id,
        s.lead_id AS lead_id,
        s.date_id AS date_id,
        s.date_id AS session_did,
        s.utm_id,
        COALESCE(s.geo_id,'na') AS geo_id,
        COALESCE(ch.vertical_type_id,s.marketed_vertical_id,0) AS marketed_vertical_id,
        0 AS vertical_type_id,
    --specific
        s.leads,
        s.valid_leads,
        --s.processed_leads,
        s.qualified_leads,
        s.offers,
        --s.closes,
        s.orders,
        s.booked_number,
        s.completed_number,
        s.potential_nmv,
        s.gmv,
        s.nmv,
        s.booked_nmv,
        s.completed_nmv,
        s.uncancelled_nmv,
        s.created_margin,
        s.completed_margin,
        s.uncancelled_margin,
        s.lc_leads,
        s.lc_valid_leads,
        s.lc_qualified_leads,
        s.lc_offers,
        s.lc_orders,
        s.lc_booked_number,
        s.lc_completed_number,
        s.lc_potential_nmv,
        s.lc_gmv,
        s.lc_nmv,
        s.lc_booked_nmv,
        s.lc_completed_nmv,
        s.lc_uncancelled_nmv,
        s.lc_created_margin,
        s.lc_completed_margin,
        s.lc_uncancelled_margin,
        s.cost,
        COALESCE(ch.pbi_utm_id,1) AS pbi_utm_id
    FROM scratch.online_conversions s
    LEFT JOIN dims.channels ch on ch.utm_id = s.utm_id
);
DROP TABLE scratch.online_conversions;









