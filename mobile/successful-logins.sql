SELECT      DATE(timestamp) as Date,
            COUNT( * ) as Total, 
            SUM( IF( event_platform LIKE 'Android%', 1, 0) ) AS Android,
            SUM( IF( event_platform LIKE 'iOS%', 1, 0) ) AS iOS

FROM        {{ tables.login_attempts }}

WHERE       event_result = 'success' AND
            wiki = 'commonswiki'
GROUP BY    DATE(timestamp)
