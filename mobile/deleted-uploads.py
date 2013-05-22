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
    Date,
    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        DATE(log_timestamp) = Date AND
        log_action = 'delete'
    ) AS Android,

    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        DATE(log_timestamp) = Date AND
        log_action = 'delete'
    ) AS iOS,
            
    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        DATE(log_timestamp) = Date AND
        log_action = 'delete'
    ) AS Web
        
-- http://stackoverflow.com/a/6871220/365238
FROM (
    SELECT DATE_FORMAT(
        ADDDATE(CURDATE() - INTERVAL {{ intervals.running_average }} - 1 DAY, @num:=@num+1),
        '%%%%Y-%%%%m-%%%%d'
    ) AS Date
    FROM logging, (SELECT @num:=-1) num LIMIT {{ intervals.running_average }}
) AS Month;
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
