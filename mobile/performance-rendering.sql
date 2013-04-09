SELECT
    DATE(timestamp) AS Date,
    AVG(event_rendering) AS Rendering
FROM {{ tables.navigation_timing }}
WHERE
    event_mobileMode = 'stable' AND
    DATE(timestamp) >= (CURDATE() - INTERVAL {{ intervals.running_average }} DAY)
GROUP BY Date;
