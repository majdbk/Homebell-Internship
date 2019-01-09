CREATE OR REPLACE FUNCTION f_parse_url_query_string(url VARCHAR(MAX))
RETURNS varchar(max)
STABLE
AS $$
    from urlparse import urlparse, parse_qsl
    import json
    url = unicode(url, errors='ignore')
    return json.dumps(dict(parse_qsl(urlparse(url)[4])))
$$ LANGUAGE plpythonu;
/*
UPDATE atomic.events
SET 
mkt_medium = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_medium') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_medium') END,
mkt_source = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_source') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_source') END,
mkt_campaign = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_campaign') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_campaign') END,
mkt_term = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_term') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_term') END,
mkt_content = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_content') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'utm_content') END,
mkt_clickid = CASE WHEN json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'gclid') = '' THEN null else json_extract_path_text( f_parse_url_query_string(COALESCE(page_url,'')),'gclid') END
WHERE mkt_source is null AND page_url like '%?%' AND collector_tstamp > '2017-06-12'
;
*/
-- FIXING the non loading of parts of the URL form the event tracker
UPDATE atomic.events
	SET 
		page_urlscheme = REGEXP_SUBSTR(page_url,'^((http[s]?|ftp):)'),
		page_urlhost = SPLIT_PART(REGEXP_SUBSTR(page_url,'([^:\/\s]+)((\/\w+)*\/)'),'/',1),
		page_urlpath = '/'||SPLIT_PART(SPLIT_PART(SPLIT_PART(page_url,'.com/',2),'?',1),'#',1),
		page_urlquery = SPLIT_PART(SPLIT_PART(page_url,'?',2),'#',1),
		page_urlfragment =CASE WHEN SPLIT_PART(page_url,'#',2) = '' THEN null else '#'||SPLIT_PART(page_url,'#',2) END
WHERE page_urlscheme is null AND collector_tstamp > '2017-05-17';