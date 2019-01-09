CREATE OR REPLACE function f_LOWER_NOT_NULL(varchar)
  returns varchar
IMMUTABLE
as $$
  SELECT COALESCE(LOWER($1),'')
$$ language sql;