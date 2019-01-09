CREATE OR REPLACE function f_to_did(timestamp)
  returns INT
IMMUTABLE
as $$
  SELECT (DATE_PART(epoch,DATE($1))/(3600*24))::INT
$$ language sql;