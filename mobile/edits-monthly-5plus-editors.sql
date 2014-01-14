SELECT
  -- Graphs which state they are timeboxed are assumed to be monthly so should only return a count
  COUNT(COALESCE(Main.count, 0)) as Count
FROM (
  SELECT
      COUNT(*) AS count
  FROM log.ServerSideAccountCreation_5487345 AS ssac
  LEFT JOIN enwiki.revision AS rev ON ssac.event_userId = rev.rev_user
  LEFT JOIN enwiki.page AS page ON rev.rev_page = page.page_id
  WHERE
      -- These expect to be substituted so they represent a month time period
      ssac.timestamp >= '{from_timestamp}' AND ssac.timestamp <= '{to_timestamp}' AND
      rev.rev_timestamp > '{from_timestamp}' AND rev.rev_timestamp < '{to_timestamp}' AND
      ssac.event_displayMobile = 1 AND
      ssac.wiki = 'enwiki' AND
      page.page_namespace = 0
  GROUP BY ssac.event_userId
  HAVING count >= 5
) AS Main
