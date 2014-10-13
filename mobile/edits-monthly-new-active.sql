SET @n = 5; /* edits threshold */
SET @u = 30; /* activity unit in days */

select sum(if(event_displaymobile = 1, 1, 0)) as mobile,
       sum(if(event_displaymobile <> 1, 1, 0)) as desktop
  from  (SELECT user_id
           FROM (SELECT rev_user as user_id,
                        count(*) AS revisions
                   FROM enwiki.revision
                  WHERE rev_timestamp BETWEEN DATE_FORMAT(DATE_SUB('{to_timestamp}', INTERVAL @u DAY), "%Y%m%d%H%i%S") AND '{to_timestamp}'
                    AND rev_user in (select log_user
                                       from enwiki.logging
                                      where log_type = 'newusers'
                                        and log_action = 'create'
                                        and log_timestamp BETWEEN DATE_FORMAT(DATE_SUB('{to_timestamp}', INTERVAL @u DAY), "%Y%m%d%H%i%S") AND '{to_timestamp}'
                                    )
                  GROUP BY rev_user

                  UNION ALL

                 SELECT ar_user as user_id,
                        count(*) AS revisions
                   FROM enwiki.archive
                  WHERE ar_timestamp BETWEEN DATE_FORMAT(DATE_SUB('{to_timestamp}', INTERVAL @u DAY), "%Y%m%d%H%i%S") AND '{to_timestamp}'
                    AND ar_user in (select log_user
                                      from enwiki.logging
                                     where log_type = 'newusers'
                                       and log_action = 'create'
                                       and log_timestamp BETWEEN DATE_FORMAT(DATE_SUB('{to_timestamp}', INTERVAL @u DAY), "%Y%m%d%H%i%S") AND '{to_timestamp}'
                                   )
                  GROUP BY ar_user

                ) AS user_revisions
          GROUP BY user_id
         HAVING SUM(revisions) >= @n
        ) as rolling_active_editors
            inner join
        log.ServerSideAccountCreation_5487345  on event_userId = rolling_active_editors.user_id
                                               AND useragent NOT LIKE "%WikipediaApp%"
                                               AND wiki = "enwiki"
                                               and timestamp between DATE_FORMAT(DATE_SUB('{to_timestamp}', INTERVAL @u+1 DAY), "%Y%m%d%H%i%S")
                                                                 AND date_format(date_add('{to_timestamp}', interval 1 day), '%Y%m%d%H%i%S')
;
