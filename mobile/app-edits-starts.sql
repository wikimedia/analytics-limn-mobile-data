-- shamelessly copied from successful-edits-main.sql

SELECT
    Month.Date,
    COALESCE(Web.Web, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
-- ... using MariaDB 10 SEQUENCE engine instead of information_schema.columns
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM seq_1_to_100, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month
LEFT JOIN (
    SELECT
        DATE(timestamp) AS Date,
        SUM(1) AS Web

    FROM {{ tables.edits_app }} WHERE
        event_action = 'start' AND
        wiki != 'testwiki'
    GROUP BY Date
) AS Web ON Month.Date = Web.Date;
