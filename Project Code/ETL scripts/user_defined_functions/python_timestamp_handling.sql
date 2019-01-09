create or replace function f_from_iso_string (a varchar)
  returns datetime
stable
as $$
  import datetime
  from datetime import date
  import dateutil.parser
  return dateutil.parser.parse(a)
$$ language plpythonu;