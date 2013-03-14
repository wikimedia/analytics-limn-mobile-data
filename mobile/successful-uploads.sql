SELECT
    Month.Date,
    COALESCE(Apps.Android, 0) + COALESCE(Apps.iOS, 0) + COALESCE(Web.Web, 0) AS Total,
    COALESCE(Apps.Android, 0) AS Android,
    COALESCE(Apps.iOS, 0) AS iOS,
    COALESCE(Web.Web, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM {{ tables.upload_attempts }}, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month
LEFT JOIN (
    SELECT
        DATE(timestamp) AS Date,
        SUM(IF( event_platform LIKE 'Android%', 1, 0)) AS Android,
        SUM(IF( event_platform LIKE 'iOS%', 1, 0)) AS iOS

    FROM {{ tables.upload_attempts }} WHERE
        event_result = 'success' AND
        wiki = 'commonswiki'
    GROUP BY Date
) AS Apps ON Month.Date = Apps.Date
LEFT JOIN (
    SELECT
        DATE(timestamp) AS Date,
        SUM(1) AS Web

    FROM {{ tables.upload_web }} WHERE
        event_action = 'success' AND
        wiki != 'testwiki'
    GROUP BY Date
) AS Web ON Month.Date = Web.Date;
