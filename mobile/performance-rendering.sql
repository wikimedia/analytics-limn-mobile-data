SELECT
    Date,

    (SELECT AVG(event_rendering) FROM {{ tables.navigation_timing }} WHERE
        event_mobileMode = 'stable' AND
        DATE(timestamp) < Date AND
        (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
    ) AS Rendering30Day,

    (SELECT AVG(event_rendering) FROM {{ tables.navigation_timing }} WHERE
        event_mobileMode = 'stable' AND
        DATE(timestamp) = Date
    ) AS Rendering

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM {{ tables.upload_attempts }}, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month;
