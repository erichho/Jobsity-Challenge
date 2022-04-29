/*
BONUS query 1: 
From the two most commonly appearing regions, which is the latest datasource?

ANS:
datasource	region	datetime
pt_search_app	Turin	2018-05-31 06:20:59

*/

SELECT d.datasource,r.region,t.datetime from (
SELECT regionID, count(*) count FROM trips 
GROUP BY regionID
ORDER BY count DESC
LIMIT 2) as top2regions
LEFT JOIN trips t
ON top2regions.regionID = t.regionID
LEFT JOIN datasources d
ON t.datasourceID = d.datasourceID
LEFT JOIN regions r
ON t.regionID=r.regionID
ORDER BY datetime desc
LIMIT 1