CREATE OR REPLACE function f_strip_special(word VARCHAR(MAX))
  returns VARCHAR(MAX)
IMMUTABLE
as $$
  if word is None: return None
  return ''.join(e for e in word if e.isalnum()).lower()
$$ language plpythonu;