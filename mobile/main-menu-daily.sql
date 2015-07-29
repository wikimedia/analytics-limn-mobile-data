SELECT
  date('{from_timestamp}') as Day,
  sum(Home) as Home,
  sum(Random) as Random,
  sum(Nearby) as Nearby,
  sum(Watchlist) as Watchlist,
  sum(Uploads) as Uploads,
  sum(Settings) as Settings,
  sum(Profile) as Profile,
  sum(Logout) as Logout,
  sum(Login) as Login,
  sum(Collections) as Collections
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
      0 as Collections
    FROM
      MobileWebClickTracking_5929948
	WHERE
	  timestamp >= '{from_timestamp}' AND
	  timestamp <= '{to_timestamp}'
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
      0 as Collections
    FROM
      MobileWebMainMenuClickTracking_10703095
	WHERE
	  timestamp >= '{from_timestamp}' AND
	  timestamp <= '{to_timestamp}'
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
      sum(if(event_name = 'collections', 1, 0)) as Collections
    FROM
      MobileWebMainMenuClickTracking_11568715
	WHERE
	  timestamp >= '{from_timestamp}' AND
	  timestamp <= '{to_timestamp}'
  )
) AS MobileWebMainMenuClickTracking
