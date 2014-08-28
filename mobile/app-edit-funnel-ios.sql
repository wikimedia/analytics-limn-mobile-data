-- derived from cancelled-uploads.sql
SELECT      SUM( IF( event_action = 'start', 1, 0) ) AS start,
            SUM( IF( event_action = 'preview', 1, 0) ) AS preview,
            SUM( IF( event_action = 'saveAttempt', 1, 0) ) AS saveAttempt,
            SUM( IF( event_action = 'saved', 1, 0) ) AS saved

FROM        {{ tables.edits_app }}

WHERE       ( userAgent LIKE '%Darwin%' OR userAgent LIKE '%iPhone OS%' )
            AND wiki != 'testwiki'
            AND timestamp >= '{from_timestamp}'
            AND timestamp < '{to_timestamp}'

GROUP BY    DATE( timestamp )
