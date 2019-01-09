CREATE OR REPLACE function f_from_did(BIGINT)
  returns DATE
IMMUTABLE
as $$
  SELECT DATEADD(day,$1,'1970-01-01')::DATE
$$ language sql;