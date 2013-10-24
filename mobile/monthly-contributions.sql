SELECT
    DATE_FORMAT(CONCAT(Date.month, '01'), '%Y-%m-%d') AS Month,
    COALESCE(Edits.EditsMain, 0) AS EditsMain,
    COALESCE(Edits.EditsOther, 0) AS EditsOther,
    COALESCE(AppUploads.Android, 0) AS Android,
    COALESCE(AppUploads.iOS, 0) AS iOS,
    COALESCE(WebUploads.Web, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT EXTRACT(YEAR_MONTH FROM SUBDATE(CURDATE(), INTERVAL @num:=@num+1 MONTH)) AS month
    FROM information_schema.columns, (SELECT @num:=-1) num LIMIT 12
) AS Date
LEFT JOIN (
    SELECT
        EXTRACT(YEAR_MONTH FROM timestamp) AS month,
        SUM(IF( event_platform LIKE 'Android%', 1, 0)) AS Android,
        SUM(IF( event_platform LIKE 'iOS%', 1, 0)) AS iOS

    FROM {{ tables.upload_attempts }} WHERE
        event_result = 'success' AND
        wiki = 'commonswiki'
    GROUP BY month
) AS AppUploads ON Date.month = AppUploads.month
LEFT JOIN (
    SELECT
        EXTRACT(YEAR_MONTH FROM timestamp) AS month,
        SUM(1) AS Web

    FROM {{ tables.upload_web }} WHERE
        event_action = 'success' AND
        wiki != 'testwiki'
    GROUP BY month
) AS WebUploads ON Date.month = WebUploads.month
LEFT JOIN (
    SELECT
        EXTRACT(YEAR_MONTH FROM timestamp) AS month,
        SUM(IF( event_namespace = 0, 1, 0)) AS EditsMain,
        SUM(IF( event_namespace != 0, 1, 0)) AS EditsOther

    FROM {{ tables.edits_web }} WHERE
        event_action = 'success' AND
        wiki != 'testwiki'
    GROUP BY month
) AS Edits ON Date.month = Edits.month;
