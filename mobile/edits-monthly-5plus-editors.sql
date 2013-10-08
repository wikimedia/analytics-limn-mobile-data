SELECT Month, COUNT(*) AS Count FROM (

SELECT
	DATE_FORMAT(CONCAT(Month.month, '01'), '%Y-%m-%d') AS Month,
	COALESCE(Main.count, 0) AS Count

-- http://stackoverflow.com/a/6871220/365238
FROM (
	SELECT EXTRACT(YEAR_MONTH FROM SUBDATE(CURDATE(), INTERVAL @num:=@num+1 MONTH)) AS month
	FROM information_schema.columns, (SELECT @num:=-1) num LIMIT 12
) AS Month
LEFT JOIN (
	SELECT
	    EXTRACT(YEAR_MONTH FROM ssac.timestamp) AS month,
	    COUNT(*) AS count
	FROM {{ tables.account_creation }} AS ssac
	LEFT JOIN enwiki.revision AS rev ON ssac.event_userId = rev.rev_user
	LEFT JOIN enwiki.page AS page ON rev.rev_page = page.page_id
	WHERE
	    ssac.event_displayMobile = 1 AND
	    ssac.wiki = 'enwiki' AND
	    page.page_namespace = 0
	GROUP BY ssac.event_userId
	HAVING count >= 5
) AS Main ON Month.month = Main.month

) AS Helper GROUP BY Month;
