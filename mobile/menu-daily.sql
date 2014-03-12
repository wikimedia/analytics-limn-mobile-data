select
    sum(if(event_name = 'hamburger-home', 1, 0)) as Home,
    sum(if(event_name = 'hamburger-random', 1, 0)) as Random,
    sum(if(event_name = 'hamburger-uploads', 1, 0)) as Uploads,
    sum(if(event_name = 'hamburger-watchlist', 1, 0)) as Watchlist,
    sum(if(event_name = 'hamburger-nearby', 1, 0)) as Nearby,
    sum(if(event_name = 'hamburger-settings', 1, 0)) as Settings,
    sum(if(event_name = 'hamburger-login', 1, 0)) as Login,
    sum(if(event_name = 'hamburger-logout', 1, 0)) as Logout,
    sum(if(event_name = 'hamburger-profile', 1, 0)) as 'Profile (Beta only)'
from
    MobileWebClickTracking_5929948
where
    timestamp >= '{from_timestamp}' and
    timestamp <= '{to_timestamp}'
