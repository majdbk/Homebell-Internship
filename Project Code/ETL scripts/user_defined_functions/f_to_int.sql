CREATE OR REPLACE function f_to_int(word VARCHAR(MAX),depth int)
  returns INT
IMMUTABLE
as $$
  if word is None: return None
  depth = depth or 3
  word = word.lower();
  breaker = filter((lambda x: x.isalnum() ), [c for c in word] )[:depth]
  breaker = map(lambda x: ord(x),breaker)
  res = 0
  i = 0;
  for x in breaker:
	  res = res + x*pow(36, i)
	  i = i + 1
  return res
$$ language plpythonu;