select
    sum(if(event_name = 'diff-thank', 1, 0)) as 'Thanks',
    sum(if(event_name = 'diff-user', 1, 0)) as 'User page link',
    sum(if(event_name = 'diff-view', 1, 0)) as 'Navigate to subject page',
    sum(if(event_name = 'diff-prev-or-next', 1, 0)) as 'Clicks previous or next (beta only)'
from
    MobileWebClickTracking_5929948
where
    timestamp >= '{from_timestamp}' and
    timestamp <= '{to_timestamp}'
