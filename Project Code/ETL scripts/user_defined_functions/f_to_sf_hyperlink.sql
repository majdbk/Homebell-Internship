CREATE OR REPLACE FUNCTION f_to_sf_hyperlink(sf_id VARCHAR(MAX), is_for_sheets BOOLEAN, alt_text VARCHAR(MAX), separator VARCHAR(1))
RETURNS varchar(max)
IMMUTABLE
AS $$
    if sf_id is None: return None
    if separator is None: separator = ','
    url = 'https://eu6.salesforce.com/'
    url += sf_id
    if alt_text == '':
        alt_text = sf_id
    if is_for_sheets:
		url = ''.join(['=HYPERLINK("',url,'"',separator,'"',alt_text,'")'])
    return url
$$ LANGUAGE plpythonu;

