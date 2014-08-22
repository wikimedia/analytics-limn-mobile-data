-- derived from cancelled-uploads.sql
SELECT      COUNT( * ) AS Total,
            SUM( IF( userAgent LIKE '%Android%', 1, 0) ) AS Android,
            SUM( IF( userAgent LIKE '%Darwin%' OR userAgent LIKE '%iPhone OS%', 1, 0) ) AS iOS

FROM        {{ tables.edits_app }} as Edits

WHERE       event_action = 'start'
            AND wiki != 'testwiki'
            AND Edits.timestamp >= '{from_timestamp}'
            AND Edits.timestamp < '{to_timestamp}'

GROUP BY    DATE( timestamp )
