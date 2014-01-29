mobile_uploads_android_sql = u"""
SELECT  event_filename
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'Android%'
        AND event_result="success"
        AND wiki="commonswiki"
"""

mobile_uploads_ios_sql = u"""
SELECT  event_filename
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'iOS%'
        AND event_result="success"
        AND wiki="commonswiki"
"""

mobile_uploads_web_sql = u"""
SELECT  log_title
FROM    logging INNER JOIN tag_summary ON log_id = ts_log_id
WHERE   tag_summary.ts_tags = 'mobile edit'
        AND log_action = 'upload'"""

deleted_file_template = u"""
SELECT
    DATE_FORMAT(CONCAT(Month.month, '01'), '%%%%Y-%%%%m-%%%%d') AS Month,
    COALESCE(Android.count, 0) AS Android,
    COALESCE(iOS.count, 0) AS iOS,
    COALESCE(Web.count, 0) AS Web

-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT EXTRACT(YEAR_MONTH FROM SUBDATE(CURDATE(), INTERVAL @num:=@num+1 MONTH)) AS month
    FROM information_schema.columns, (SELECT @num:=-1) num LIMIT 12
) AS Month

LEFT JOIN (
    SELECT EXTRACT(YEAR_MONTH FROM log_timestamp) AS month, COUNT(*) AS count
    FROM logging
    WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'delete'
    GROUP BY month
) AS Android ON Month.month = Android.month

LEFT JOIN (
    SELECT EXTRACT(YEAR_MONTH FROM log_timestamp) AS month, COUNT(*) AS count
    FROM logging
    WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'delete'
    GROUP BY month
) AS iOS ON Month.month = iOS.month

LEFT JOIN (
    SELECT EXTRACT(YEAR_MONTH FROM log_timestamp) AS month, COUNT(*) AS count
    FROM logging
    WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'delete'
    GROUP BY month
) AS Web ON Month.month = Web.month;
"""

headers = ["date", "Android", "iOS", "Web"]


def titles_for(dg, sql, src_db="el"):
    titles = []
    cur = dg.get_connection(src_db).cursor()

    cur.execute(dg.render(sql))

    for row in cur:
        if isinstance(row[0], str):
            title = row[0].decode('utf-8')
        else:
            title = row[0]
        title = title[0].upper() + title[1:]
        titles.append(title.replace("File:", "").replace(" ", "_"))

    return titles


def execute(dg):

    results = []

    android_titles = titles_for(dg, mobile_uploads_android_sql)
    iOS_titles = titles_for(dg, mobile_uploads_ios_sql)
    web_titles = titles_for(dg, mobile_uploads_web_sql, "commons")

    deletes_sql = deleted_file_template % (
        ('%s,' * len(android_titles))[:-1],
        ('%s,' * len(iOS_titles))[:-1],
        ('%s,' * len(web_titles))[:-1]
    )

    commons_cur = dg.get_connection('commons').cursor()
    commons_cur.execute(dg.render(deletes_sql), android_titles + iOS_titles + web_titles)

    results = commons_cur.fetchall()

    return (headers, results)
