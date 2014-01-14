SELECT
  sum(case when Main.isMobile = 1 then 1 else 0 end) as mobileEdits,
  sum(case when Main.isMobile = 0 then 1 else 0 end) as desktopEdits
FROM (
  SELECT
    ssac.event_userID,
    Newbies.event_isMobile as isMobile,
    count(*) as count
  FROM log.ServerSideAccountCreation_5487345 AS ssac
  LEFT JOIN NewEditorEdit_6792669 as Newbies ON ssac.event_userId = Newbies.event_userId
  WHERE
    ssac.timestamp >= '{from_timestamp}' and ssac.timestamp < '{to_timestamp}' AND
    Newbies.timestamp >= '{from_timestamp}' and Newbies.timestamp < '{to_timestamp}' AND
    ssac.wiki = 'enwiki'
  group by ssac.event_userID, Newbies.event_isMobile
) AS Main where Main.count > 5
