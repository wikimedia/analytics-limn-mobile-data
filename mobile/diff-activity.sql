SELECT
  date('{from_timestamp}') as Day,
  sum(view) as 'View page link',
  sum(user) as 'User page link',
  sum(prev_or_next) as 'Clicks previous or next (beta only)',
  sum(thank) as 'Thanks'
FROM (
  (
    SELECT
      sum(if(event_name = 'diff-view', 1, 0)) as view,
      sum(if(event_name = 'diff-user', 1, 0)) as user,
      sum(if(event_name = 'diff-prev-or-next', 1, 0)) as prev_or_next,
      sum(if(event_name = 'diff-thank', 1, 0)) as thank
    FROM
      MobileWebClickTracking_5929948
    WHERE
      timestamp >= '{from_timestamp}' and
      timestamp <= '{to_timestamp}'
  )
  UNION
  (
    SELECT
      sum(if(event_name = 'view', 1, 0)) as view,
      sum(if(event_name = 'user', 1, 0)) as user,
      sum(if(event_name = 'prev-or-next', 1, 0)) as prev_or_next,
      sum(if(event_name = 'thank', 1, 0)) as thank
    FROM
      MobileWebDiffClickTracking_10720373
    WHERE
      timestamp >= '{from_timestamp}' and
      timestamp <= '{to_timestamp}'
  )
) AS MobileWebDiffClickTracking
