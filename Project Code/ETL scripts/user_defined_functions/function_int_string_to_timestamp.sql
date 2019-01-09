create function f_int_string_to_timestamp (varchar)
  returns timestamp
stable
as $$
  SELECT CASE WHEN $1 LIKE '____-__-__' 
  THEN $1::timestamp 
  ELSE (TIMESTAMP 'epoch' + SUBSTRING($1,0,11)::int * INTERVAL '1 Second ') 
  END  
$$ language sql; 