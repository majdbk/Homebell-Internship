CREATE OR REPLACE function f_decode_url(word VARCHAR(MAX))
  returns VARCHAR(MAX)
IMMUTABLE
as $$
  from urlparse import unquote
  if word is None: return None
  return unquote(word) 
$$ language plpythonu;