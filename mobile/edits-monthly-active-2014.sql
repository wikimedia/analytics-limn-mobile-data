 SELECT sum(case when Main.isMobile = 1 then 1 else 0 end) as mobileEdits,
        sum(case when Main.isMobile = 0 then 1 else 0 end) as desktopEdits
   FROM (SELECT Newbies.event_userID,
                Newbies.event_isMobile as isMobile,
                count(*) as count
           FROM NewEditorEdit_6792669 as Newbies
          WHERE Newbies.event_userAge <= 2592000
            and Newbies.timestamp >= '{from_timestamp}'
            and Newbies.timestamp < '{to_timestamp}'
            and Newbies.wiki = 'enwiki'
          group by
                Newbies.event_userID,
                Newbies.event_isMobile
        ) AS Main
  WHERE Main.count > 5
