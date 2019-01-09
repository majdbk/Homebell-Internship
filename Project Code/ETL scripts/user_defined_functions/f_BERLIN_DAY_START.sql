CREATE OR REPLACE function f_BERLIN_DAY_START(TIMESTAMP)
  returns TIMESTAMP
IMMUTABLE
as $$
  SELECT CONVERT_TIMEZONE('Europe/Berlin','UTC', DATE_TRUNC('day',$1) )
$$ language sql;