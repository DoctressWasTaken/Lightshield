SELECT platform,
       DATE_TRUNC('minute', interval_time)                        AS interval_time,
       SUM(request_count) / 60.0,
       SUM(CASE WHEN response_type != 200 THEN request_count END) AS non_200
FROM tracking.requests
WHERE endpoint = 'server'
  AND (LENGTH(platform) > 4 OR platform = 'asia')
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 500;