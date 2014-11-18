select
    sum(if(event_name = 'hamburger', 1, 0)) as Hamburger,
    sum(if(event_name = 'hamburger-home', 1, 0)) as Home,
    sum(if(event_name = 'hamburger-random', 1, 0)) as Random,
    sum(if(event_name = 'hamburger-uploads', 1, 0)) as Uploads,
    sum(if(event_name = 'hamburger-watchlist', 1, 0)) as Watchlist,
    sum(if(event_name = 'hamburger-nearby', 1, 0)) as Nearby,
    sum(if(event_name = 'hamburger-settings', 1, 0)) as Settings,
    sum(if(event_name = 'hamburger-login', 1, 0)) as Login,
    sum(if(event_name = 'hamburger-logout', 1, 0)) as Logout,
    sum(if(event_name = 'hamburger-profile', 1, 0)) as Profile,
    sum(if(event_name = 'search', 1, 0)) as Search,
    sum(if(event_name = 'notifications', 1, 0)) as Notifications,
    sum(if(event_name = 'reference', 1, 0)) as Reference,
    sum(if(event_name = 'languages', 1, 0)) as Languages,
    sum(if(event_name = 'lastmodified-history', 1, 0)) as 'Last modified history',
    sum(if(event_name = 'lastmodified-profile', 1, 0)) as 'Last modified profile',
    sum(if(event_name = 'page-toc-toggle', 1, 0)) as 'Page TOC toggle',
    sum(if(event_name = 'page-toc-link', 1, 0)) as 'Page TOC link'
from
    MobileWebClickTracking_5929948
where
    timestamp >= '{from_timestamp}' and
    timestamp <= '{to_timestamp}'