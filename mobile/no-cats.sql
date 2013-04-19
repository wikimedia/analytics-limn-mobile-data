SELECT
    Date,
    (
        (SELECT SUM(`event_files-count`) FROM {{ tables.cat_attempts }} WHERE
            event_platform LIKE 'Android%' AND
            `event_categories-count` = 0 AND
            wiki = 'commonswiki' AND
            DATE(timestamp) < Date AND
            (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
        ) /
        (SELECT SUM(`event_files-count`) FROM {{ tables.cat_attempts }} WHERE
            event_platform LIKE 'Android%' AND
            wiki = 'commonswiki' AND
            DATE(timestamp) < Date AND
            (timestamp + INTERVAL 0 DAY) > (Date - INTERVAL {{ intervals.running_average }} DAY)
        )
    ) AS Android

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%Y-%m-%d'
    ) AS Date
    FROM {{ tables.upload_attempts }}, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month;
