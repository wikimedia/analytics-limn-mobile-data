SELECT
    Month.Date,
    COALESCE(Web.Web, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM information_schema.columns, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month
LEFT JOIN (
    SELECT
        DATE(timestamp) AS Date,
        COUNT(DISTINCT event_username) AS Web

    FROM {{ tables.edits_web }} WHERE
        event_action = 'success' AND
        event_namespace = 0 AND
        wiki != 'testwiki'
    GROUP BY Date
) AS Web ON Month.Date = Web.Date;