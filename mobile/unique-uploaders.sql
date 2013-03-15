SELECT
    Date,

    (
        (SELECT COUNT(DISTINCT event_username) FROM {{ tables.upload_attempts }} WHERE
            event_result = 'success' AND
            wiki = 'commonswiki' AND
            DATE(timestamp) < Date AND
            (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
        ) +
        (SELECT COUNT(DISTINCT event_token) FROM {{ tables.upload_web }} WHERE
            event_action = 'success' AND
            wiki != 'testwiki' AND
            DATE(timestamp) < Date AND
            (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
        )
    ) AS Total,

    (SELECT COUNT(DISTINCT event_username) FROM {{ tables.upload_attempts }} WHERE
        event_platform LIKE 'Android%' AND
        event_result = 'success' AND
        wiki = 'commonswiki' AND
        DATE(timestamp) < Date AND
        (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
    ) AS Android,

    (SELECT COUNT(DISTINCT event_username) FROM {{ tables.upload_attempts }} WHERE
        event_platform LIKE 'iOS%' AND
        event_result = 'success' AND
        wiki = 'commonswiki' AND
        DATE(timestamp) < Date AND
        (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
    ) AS iOS,

    (SELECT COUNT(DISTINCT event_token) FROM {{ tables.upload_web }} WHERE
        event_action = 'success' AND
        wiki != 'testwiki' AND
        DATE(timestamp) < Date AND
        (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
    ) AS Web

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM {{ tables.upload_attempts }}, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month;
