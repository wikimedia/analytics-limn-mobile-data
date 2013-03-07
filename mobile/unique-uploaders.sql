SELECT      DATE(T1.timestamp) as Date,

            (SELECT COUNT( DISTINCT T2.event_username ) 
             FROM   {{ tables.upload_attempts }} AS T2
             WHERE  T2.timestamp < T1.timestamp AND
                    (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average }} )) AS Total,

            (SELECT COUNT( DISTINCT T2.event_username ) 
             FROM   {{ tables.upload_attempts }} AS T2
             WHERE  T2.event_platform LIKE 'Android%' AND
                    T2.timestamp < T1.timestamp AND
                    (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average }} )) AS Android,

            (SELECT COUNT( DISTINCT T2.event_username ) 
             FROM   {{ tables.upload_attempts }} AS T2
             WHERE  T2.event_platform LIKE 'iOS%' AND
                    T2.timestamp < T1.timestamp AND
                    (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average}} )) AS iOS
            
FROM        {{ tables.upload_attempts }} AS T1
WHERE       T1.event_result = 'success' AND
            T1.wiki = 'commonswiki'
GROUP BY    DATE(T1.timestamp)
