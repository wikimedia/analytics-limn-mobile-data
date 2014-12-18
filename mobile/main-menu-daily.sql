SELECT
  sum(Home) as Home,
  sum(Random) as Random,
  sum(Nearby) as Nearby,
  sum(Watchlist) as Watchlist,
  sum(Uploads) as Uploads,
  sum(Settings) as Settings,
  sum(Profile) as Profile,
  sum(Logout) as Logout,
  sum(Login) as Login
FROM (
  (
    SELECT
      sum(if(event_name = 'hamburger-home', 1, 0)) as Home,
      sum(if(event_name = 'hamburger-random', 1, 0)) as Random,
      sum(if(event_name = 'hamburger-nearby', 1, 0)) as Nearby,
      sum(if(event_name = 'hamburger-watchlist', 1, 0)) as Watchlist,
      sum(if(event_name = 'hamburger-uploads', 1, 0)) as Uploads,
      sum(if(event_name = 'hamburger-settings', 1, 0)) as Settings,
      sum(if(event_name = 'hamburger-profile', 1, 0)) as Profile,
      sum(if(event_name = 'hamburger-logout', 1, 0)) as Logout,
      sum(if(event_name = 'hamburger-login', 1, 0)) as Login,
      timestamp
    FROM
      MobileWebClickTracking_5929948
  )
  UNION
  (
    SELECT
      sum(if(event_name = 'home', 1, 0)) as Home,
      sum(if(event_name = 'random', 1, 0)) as Random,
      sum(if(event_name = 'nearby', 1, 0)) as Nearby,
      sum(if(event_name = 'watchlist', 1, 0)) as Watchlist,
      sum(if(event_name = 'uploads', 1, 0)) as Uploads,
      sum(if(event_name = 'settings', 1, 0)) as Settings,
      sum(if(event_name = 'profile', 1, 0)) as Profile,
      sum(if(event_name = 'logout', 1, 0)) as Logout,
      sum(if(event_name = 'login', 1, 0)) as Login,
      timestamp
    FROM
      MobileWebMainMenuClickTracking_10703095
  )
) AS MobileWebMainMenuClickTracking
WHERE
    MobileWebMainMenuClickTracking.timestamp >= '{from_timestamp}' AND
    MobileWebMainMenuClickTracking.timestamp <= '{to_timestamp}'
