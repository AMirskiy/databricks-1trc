SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta`
GROUP BY station
ORDER BY station;