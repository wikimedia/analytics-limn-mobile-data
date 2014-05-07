SELECT
    Month.date AS date,
    COALESCE(Android.count, 0) + COALESCE(iOS.count, 0) + COALESCE(Web.count, 0) AS Total,
    COALESCE(Android.count, 0) AS Android,
    COALESCE(iOS.count, 0) AS iOS,
    COALESCE(Web.count, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
-- ... using MariaDB 10 SEQUENCE engine instead of information_schema.columns
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS date
    FROM seq_1_to_100, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month

LEFT JOIN (
    SELECT DATE(timestamp) AS date, COUNT(DISTINCT event_username) AS count
    FROM {{ tables.upload_attempts }}
    WHERE
        event_platform LIKE 'Android%' AND
        event_result = 'success' AND
        wiki = 'commonswiki'
    GROUP BY date
    ORDER BY date DESC
    LIMIT {{ intervals.running_average }}
) AS Android ON Month.date = Android.date

LEFT JOIN (
    SELECT DATE(timestamp) AS date, COUNT(DISTINCT event_username) AS count
    FROM {{ tables.upload_attempts }}
    WHERE
        event_platform LIKE 'iOS%' AND
        event_result = 'success' AND
        wiki = 'commonswiki'
    GROUP BY date
    ORDER BY date DESC
    LIMIT {{ intervals.running_average }}
) AS iOS ON Month.date = iOS.date

LEFT JOIN (
    SELECT DATE(timestamp) AS date, COUNT(DISTINCT event_username) AS count
    FROM {{ tables.upload_web }}
    WHERE
        event_action = 'success' AND
        wiki != 'testwiki'
    GROUP BY date
    ORDER BY date DESC
    LIMIT {{ intervals.running_average }}
) AS Web ON Month.date = Web.date;
