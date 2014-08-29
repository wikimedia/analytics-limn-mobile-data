-- Get number of blocked app users on enwiki
-- For this report:
-- you are considered an 'app user' if you've created the account on the Android or iOS native app

SELECT COUNT( 1 ) AS NumBlocks
FROM
  (
    SELECT ipb_user,
           ipb_timestamp
    FROM enwiki.ipblocks
    WHERE ipb_timestamp >= '{from_timestamp}'
      AND ipb_timestamp <= '{to_timestamp}'
  ) blocks

    -- only need ServerSideAccountCreation_5487345 and later since the earliest
    -- account creation there was before the native apps were published
  INNER JOIN ServerSideAccountCreation_5487345 ac
          ON blocks.ipb_user = ac.event_userId

WHERE ac.wiki = 'enwiki'

GROUP BY DATE( ipb_timestamp )
