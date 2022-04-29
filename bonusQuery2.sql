/*
BONUS query 2: 
What regions has the "cheap_mobile" datasource appeared in?

ANS:
region
Prague
Turin
Hamburg

*/

SELECT DISTINCT(r.region) from trips t
LEFT JOIN datasources d
ON t.datasourceID = d.datasourceID
LEFT JOIN regions r
ON t.regionID = r.regionID
WHERE d.datasource = 'cheap_mobile'