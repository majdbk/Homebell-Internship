

BEGIN;
ALTER TABLE dims.vertical_type  RENAME TO vertical_type_old;
CREATE TABLE dims.vertical_type 
	DISTSTYLE ALL
	SORTKEY(vertical_type_id)
AS (
	WITH vertical_names AS (
		SELECT DISTINCT vertical
		FROM (
			SELECT DISTINCT product 
			AS vertical 
			FROM dims.channels
				UNION ALL
			SELECT DISTINCT lead_main_vertical AS vertical FROM dims.leads
		)
	),
	prep AS (
		SELECT 0::int AS job_type_id, 'other' AS job_type
		UNION ALL
		SELECT 1::int AS job_type_id, 'labour' AS job_type
		UNION ALL
		SELECT 2::int AS job_type_id, 'material' AS job_type
		UNION ALL
		SELECT 3::int AS job_type_id, 'service' AS job_type
	),
	crossed AS (
		SELECT 
			((v.id*10) + j.job_type_id)::int AS vertical_type_id,
			v.id AS vertical_id,
			v.name vertical,
			j.job_type_id,
			j.job_type,
			area_type_id,
			TRUE AS is_valid_vertical
		FROM raw_data.hb_verticals v 
		CROSS JOIN prep j
		UNION ALL
		SELECT f_TO_INT(vertical,4) AS vertical_type_id, 
			0 as vertical_id, 
			CASE WHEN vertical LIKE 'varn%' THEN 'lacquering' ELSE vertical END AS vertical, 
			0 AS job_type_id,
			'na' AS job_type,
			0 AS area_type_id,
			FALSE AS is_valid_vertical
		FROM vertical_names
	),
	combined AS (
	SELECT 
		vertical_type_id,
		vertical_id,
		vertical,
		job_type_id,
		job_type,
		area_type_id,
		CASE 
			WHEN vertical IN ('painting', 'wallpapering', 'lacquering') THEN 'interior_colouring'
			WHEN vertical IN ('exterior_painting', 'scaffolding','paint_outside_items') THEN 'exterior_colouring'
			WHEN vertical IN ('laminate', 'carpet', 'pvc', 'vinyl') THEN 'non_parquet_flooring'
			WHEN vertical = 'parquet' THEN 'parquet' 
			WHEN vertical IN ('exterior_plastering','plastering') THEN 'plastering'
			WHEN vertical IN ('tiling','wall_tiling','floor_tiling') THEN 'tiling'
			WHEN vertical IN ('constructional_plastering') THEN 'dry_walling'
			ELSE 'other'
		END
		AS category,
		CASE 
			WHEN vertical IN ('exterior_plastering','plastering','constructional_plastering','tiling','wall_tiling','floor_tiling','painting', 'wallpapering', 'lacquering','exterior_painting', 'scaffolding','paint_outside_items') 
				OR vertical LIKE 'paint%'
			THEN 'wall'
			WHEN vertical IN ('parquet','laminate', 'carpet', 'pvc', 'vinyl') 
				OR vertical LIKE 'floor%'
			THEN 'floor'
			ELSE 'other'
		END
		AS surface_type,
		is_valid_vertical,
		is_valid_vertical::INT AS bin_valid_vertical,
		ROW_NUMBER() OVER (PARTITION BY vertical_type_id ORDER BY LEN(vertical)) AS ranking -- shortest vertical name
	FROM crossed
	) 
	SELECT * 
	FROM combined -- prevent varchar compaints
	WHERE ranking = 1
);
DROP TABLE IF EXISTS dims.vertical_type_old;
-- INSERT special / DUMMY rows
INSERT INTO dims.vertical_type VALUES
(1,0,'na',0,0,0,'na','na',FALSE,0,1),
(2,null,'mixed',0,0,0,'mixed','mixed',FALSE,0,1);
END;
