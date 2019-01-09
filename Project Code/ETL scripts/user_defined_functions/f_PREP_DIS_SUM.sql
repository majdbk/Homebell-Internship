create function f_PREP_DIS_SUM(FLOAT,VARCHAR)
  returns DECIMAL(38,0)
stable
as $$
  SELECT (COALESCE($1,0)*1000000.0)::DECIMAL(38,0) 
	+ STRTOL(LEFT(MD5(COALESCE($2,'')),15),16)::DECIMAL(38,0) * 1.0e8
	+ STRTOL(RIGHT(MD5(COALESCE($2,'')),15),16)::DECIMAL(38,0)
$$ language sql; 