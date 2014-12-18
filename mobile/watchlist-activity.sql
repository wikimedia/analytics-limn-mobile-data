SELECT
      sum(a_z_switch) as 'Switched to feed',
      sum(a_z_view) as 'View page (A-Z)',
      sum(a_z_unwatch) as 'Unwatch page',
      sum(a_z_watch) as 'Watch page',
      sum(a_z_more) as 'More link on A-Z view',
      sum(feed_filter) as 'Filtered feed (feed)',
      sum(feed_switch) as 'Switched to list',
      sum(feed_view) as 'View diff (feed)'
FROM (
  (
    SELECT
      sum(if(event_name = 'watchlist-a-z-switch', 1, 0)) as a_z_switch,
      sum(if(event_name = 'watchlist-a-z-view', 1, 0)) as a_z_view,
      sum(if(event_name = 'watchlist-a-z-unwatch', 1, 0)) as a_z_unwatch,
      sum(if(event_name = 'watchlist-a-z-watch', 1, 0)) as a_z_watch,
      sum(if(event_name = 'watchlist-a-z-more', 1, 0)) as a_z_more,
      sum(if(event_name = 'watchlist-feed-filter', 1, 0)) as feed_filter,
      sum(if(event_name = 'watchlist-feed-switch', 1, 0)) as feed_switch,
      sum(if(event_name = 'watchlist-feed-view', 1, 0)) as feed_view,
      timestamp
    FROM
      MobileWebClickTracking_5929948
  )
  UNION
  (
    SELECT
      sum(if(event_name = 'watchlist-a-z-switch', 1, 0)) as a_z_switch,
      sum(if(event_name = 'watchlist-a-z-view', 1, 0)) as a_z_view,
      sum(if(event_name = 'watchlist-a-z-unwatch', 1, 0)) as a_z_unwatch,
      sum(if(event_name = 'watchlist-a-z-watch', 1, 0)) as a_z_watch,
      sum(if(event_name = 'watchlist-a-z-more', 1, 0)) as a_z_more,
      sum(if(event_name = 'watchlist-feed-filter', 1, 0)) as feed_filter,
      sum(if(event_name = 'watchlist-feed-switch', 1, 0)) as feed_switch,
      sum(if(event_name = 'watchlist-feed-view', 1, 0)) as feed_view,
      timestamp
    FROM
      MobileWebWatchlistClickTracking_10720361
  )
) AS MobileWebWatchlistClickTracking
WHERE
    MobileWebWatchlistClickTracking.timestamp >= '{from_timestamp}' and
    MobileWebWatchlistClickTracking.timestamp <= '{to_timestamp}'
