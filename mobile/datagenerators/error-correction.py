# This isn't run all the time, since the required output (the difference) is more or less constant
# In the very beginning of the apps, we uploaded a bunch of files to testwiki but still recorded
# them as commonswiki. This was fixed soon, but left a constant number of testwiki uploads
# tagged as commonswiki uploads, polluting stats
# This one just counts those, and removes that number from any stats that count 'all time' stats
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
        And wiki="commonswiki"
"""

file_actually_uploaded_sql = u"""
SELECT
    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'upload'
    ) AS Android,

    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'upload'
    ) AS iOS
"""

file_moved_sql = u"""
SELECT
    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'move'
    ) AS Android,

    (SELECT COUNT(*) FROM logging WHERE
        log_namespace = 6 AND
        log_title IN (%s) AND
        log_action = 'move'
    ) AS iOS
"""

files_existing_iOS = u"""
SELECT page_title
FROM  page JOIN categorylinks
ON cl_from = page_id
WHERE cl_to = "Uploaded_with_Mobile/iOS"
"""

files_existing_android = u"""
SELECT page_title
FROM  page JOIN categorylinks
ON cl_from = page_id
WHERE cl_to = "Uploaded_with_Mobile/Android"
"""

headers = ["", "Android", "iOS"]

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
    android_titles = titles_for(dg, mobile_uploads_android_sql)
    iOS_titles = titles_for(dg, mobile_uploads_ios_sql)

    moves_sql = file_moved_sql % (
            ('%s,' * len(android_titles))[:-1],
            ('%s,' * len(iOS_titles))[:-1]
            )

    deletes_sql = file_actually_uploaded_sql % (
            ('%s,' * len(android_titles))[:-1],
            ('%s,' * len(iOS_titles))[:-1]
            )

    commons_cur = dg.get_connection('commons').cursor()
    commons_cur.execute(dg.render(deletes_sql), android_titles + iOS_titles)

    android_actual, iOS_actual = commons_cur.fetchone()

    moves_cur = dg.get_connection('commons').cursor()
    moves_cur.execute(moves_sql, android_titles + iOS_titles)

    android_moved, iOS_moved = moves_cur.fetchone() 

    data = [
            ("Total on EL", len(android_titles), len(iOS_titles)),
            ("Uploaded on Commons", android_actual, iOS_actual),
            ("Difference", len(android_titles) - android_actual, len(iOS_titles) - iOS_actual)
            ]
    return (headers, data)
