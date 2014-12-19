SELECT
  sum(Hamburger) as Hamburger,
  sum(Search) as Search,
  sum(Notifications) as Notifications,
  sum(lastmodified_history) as 'Last modified history',
  sum(lastmodified_profile) as 'Last modified profile',
  sum(page_toc_toggle) as 'Page TOC toggle',
  sum(page_toc_link) as 'Page TOC link',
  sum(Languages) as Languages,
  sum(Reference) as Reference,
  sum(nearby_button) as 'Nearby Button',
  sum(category_button) as 'Category Button',
  sum(fontchanger_menu) as 'Font Changer menu',
  sum(fontchanger_font_change) as 'Font Changer change font button'
FROM (
  (
    SELECT
      sum(if(event_name = 'hamburger', 1, 0)) as Hamburger,
      sum(if(event_name = 'search', 1, 0)) as Search,
      sum(if(event_name = 'notifications', 1, 0)) as Notifications,
      sum(if(event_name = 'lastmodified-history', 1, 0)) as lastmodified_history,
      sum(if(event_name = 'lastmodified-profile', 1, 0)) as lastmodified_profile,
      sum(if(event_name = 'page-toc-toggle', 1, 0)) as page_toc_toggle,
      sum(if(event_name = 'page-toc-link', 1, 0)) as page_toc_link,
      sum(if(event_name = 'languages', 1, 0)) as Languages,
      sum(if(event_name = 'reference', 1, 0)) as Reference,
      sum(if(event_name = 'nearby-button', 1, 0)) as nearby_button,
      sum(if(event_name = 'category-button', 1, 0)) as category_button,
      sum(if(event_name = 'fontchanger-menu', 1, 0)) as fontchanger_menu,
      sum(if(event_name = 'fontchanger-font-change', 1, 0)) as fontchanger_font_change
    FROM
      MobileWebClickTracking_5929948
	WHERE
		timestamp >= '{from_timestamp}' AND
		timestamp <= '{to_timestamp}'
  )
  UNION
  (
    SELECT
      sum(if(event_name = 'hamburger', 1, 0)) as Hamburger,
      sum(if(event_name = 'search', 1, 0)) as Search,
      sum(if(event_name = 'notifications', 1, 0)) as Notifications,
      sum(if(event_name = 'lastmodified-profile', 1, 0)) as lastmodified_profile,
      sum(if(event_name = 'lastmodified-history', 1, 0)) as lastmodified_history,
      sum(if(event_name = 'page-toc-toggle', 1, 0)) as page_toc_toggle,
      sum(if(event_name = 'page-toc-link', 1, 0)) as page_toc_link,
      sum(if(event_name = 'languages', 1, 0)) as Languages,
      sum(if(event_name = 'reference', 1, 0)) as Reference,
      sum(if(event_name = 'nearby-button', 1, 0)) as nearby_button,
      sum(if(event_name = 'category-button', 1, 0)) as category_button,
      sum(if(event_name = 'fontchanger-menu', 1, 0)) as fontchanger_menu,
      sum(if(event_name = 'fontchanger-font-change', 1, 0)) as fontchanger_font_change
    FROM
      MobileWebUIClickTracking_10742159
	WHERE
		timestamp >= '{from_timestamp}' AND
		timestamp <= '{to_timestamp}'
  )
) AS MobileWebUIClickTracking
