
-- CONVERSION PATH

DROP TABLE IF EXISTS refined.conversion_path;
CREATE TABLE refined.conversion_path
	DISTKEY (conversion_path_id)
	SORTKEY (conversion_path_id,session_id)
AS (
	--- conversion type split due to splitting 90 periods for first sessions

	--to be done reduce un needed paths
	WITH conversion_prep AS (
		SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY conversion_path_id 
			ORDER BY c.collector_tstamp
			) as rank
		FROM refined.onsite_conversions as c
		WHERE c.session_id is not null 
	),


	conversions_split AS(
		SELECT 
		c.*,
		CASE 
			WHEN LAG(c.collector_tstamp) OVER (
				PARTITION BY c.domain_userid,c.conversion_type
				ORDER BY c.collector_tstamp
				) IS NOT NULL
			THEN LAG(c.collector_tstamp) OVER (
				PARTITION BY c.domain_userid,c.conversion_type
				ORDER BY c.collector_tstamp
				)
			ELSE c.collector_tstamp - '90days'::interval - '1second'::interval --extra offset
		END AS lookback_window,-- lookback window col will help create the lookback and split it if 
		c.collector_tstamp - '90days'::interval - '1second'::interval AS ninty_days
		FROM conversion_prep as c
		-- remove duplication only one path for each session || conversion type
		WHERE rank = 1
	),

	prep_1 AS(
		Select
			c.conversion_path_id,
			c.conversion_type,
			s.session_id,
			s.domain_userid,
			s.utm_id = MD5('') as is_direct,
		 	ROW_NUMBER() OVER (
		 		PARTITION BY s.domain_userid,c.conversion_path_id
		 		ORDER BY s.start_collector_tstamp
		 	) as path_position
		FROM conversions_split AS c
		INNER JOIN refined.sessions s ON c.domain_userid = s.domain_userid
			AND s.start_collector_tstamp > (CASE WHEN c.lookback_window > c.ninty_days THEN c.lookback_window ELSE c.ninty_days END)
			AND s.start_collector_tstamp <= c.collector_tstamp
	),
	prep AS(
		Select
			c.*,
		 	COUNT(c.session_id) OVER (
		 		PARTITION BY c.domain_userid,c.conversion_path_id
		 		ROWS BETWEEN  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		 	) as path_length
		FROM prep_1 AS c
	),
	custom_attr AS ( 
		SELECT 
		*,
		conversion_path_id||'-'||path_position as conversion_path_part_id,
		CASE 
			WHEN path_position = 1 THEN 'first'
			WHEN path_position = path_length THEN 'last'
			ELSE 'middle'  
		END AS interaction_type,
		CASE 
			WHEN path_length = 1 THEN 1
			WHEN path_position = 1 OR path_position = path_length
			THEN 0.5 
			ELSE 0 
		END AS first_last,
		CASE 
			WHEN path_length = 1 THEN 1
			WHEN path_length = 2 THEN 0.5
			WHEN path_position = 1 OR path_position = path_length
			THEN 0.4
			ELSE (0.2/(path_length-2))
		END AS position_based,
		CASE 
			WHEN path_position = 1 
			THEN 1.0 ELSE 0 
		END AS first,
		CASE 
			WHEN path_position = path_length
			THEN 1.0 ELSE 0 
		END AS last,
		1.0/path_length as linear,
		CASE 
			WHEN path_length = 1 THEN 1
			WHEN is_direct AND path_position != 1 THEN 0
			WHEN path_length = 2 THEN 0.5
			WHEN path_position = 1 OR path_position = path_length
			THEN 0.4
			ELSE (0.2/(path_length-2))
		END AS custom_prep
	FROM prep 
	)
	SELECT 
		*,
		ROUND(custom_prep/SUM(custom_prep) 
			OVER (PARTITION BY 
				conversion_path_id 
				ORDER BY path_position 
				ROWS BETWEEN  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),4)
		as custom
	FROM custom_attr	
);






