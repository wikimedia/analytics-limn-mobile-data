select
    sum(if(event_name = 'watchlist-a-z-more', 1, 0)) as 'More link on A-Z view',
    sum(if(event_name = 'watchlist-a-z-switch', 1, 0)) as 'Switched to feed',
    sum(if(event_name = 'watchlist-feed-switch', 1, 0)) as 'Switched to list',
    sum(if(event_name = 'watchlist-a-z-unwatch', 1, 0)) as 'Unwatch page',
    sum(if(event_name = 'watchlist-a-z-view', 1, 0)) as 'View page (A-Z)',
    sum(if(event_name = 'watchlist-feed-view', 1, 0)) as 'View diff (feed)',
    sum(if(event_name = 'watchlist-feed-filter', 1, 0)) as 'Filtered feed (feed)'
from 
    MobileWebClickTracking_5929948
where
    timestamp >= '{from_timestamp}' and
    timestamp <= '{to_timestamp}'
