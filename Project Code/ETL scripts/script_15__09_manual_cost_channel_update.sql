

DELETE FROM dims.channels 
WHERE utm_id IN (SELECT utm_id FROM refined.sheets_mkt_spend);

INSERT INTO dims.channels

(
	utm_id, channel, mkt_channel,mkt_subchannel,source
)
SELECT 
	 utm_id ::VARCHAR(32) as utm_id,
	channel ::VARCHAR(15) as channel,
	channel ::VARCHAR(22) as mkt_channel,
	subchannel ::VARCHAR(32) AS mkt_subchannel,
	source ::VARCHAR(128) AS source
from refined.sheets_mkt_spend
group by 1,2,3,4,5;
	
	


