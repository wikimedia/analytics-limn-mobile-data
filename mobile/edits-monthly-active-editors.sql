SELECT
  DATE_FORMAT(CONCAT(Month.month, '01'), '%Y-%m-%d') AS Month,
  COALESCE(COUNT(*), 0) AS Count

-- http://stackoverflow.com/a/6871220/365238
-- ... using MariaDB 10 SEQUENCE engine instead of information_schema.columns
FROM (
  SELECT EXTRACT(YEAR_MONTH FROM SUBDATE(CURDATE(), INTERVAL @num:=@num+1 MONTH)) AS month
  FROM seq_1_to_12, (SELECT @num:=-1) num LIMIT 12
) AS Month

LEFT JOIN (
  SELECT
    EXTRACT(YEAR_MONTH FROM timestamp) AS month,
    event_username,
    count(*) AS count
  FROM {{ tables.edits_web }}
  WHERE event_action = 'success'
  GROUP BY month, event_username
  HAVING count > 5
) AS Main ON Month.month = Main.month
GROUP BY Month;
