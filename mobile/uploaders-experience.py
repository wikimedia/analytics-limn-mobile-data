mobile_uploaders_android_sql = u"""
SELECT  event_username, count(*) AS uploads
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'Android%'
        AND event_result="success"
        AND wiki="commonswiki"
GROUP BY event_username"""

mobile_uploaders_ios_sql = u"""
SELECT  event_username, count(*) AS uploads
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'iOS%'
        AND event_result="success"
        AND wiki="commonswiki"
GROUP BY event_username"""

mobile_uploaders_web_sql = u"""
SELECT  event_username, count(*) AS uploads
FROM    {{tables.upload_web}}
WHERE   event_action="success"
        AND wiki != "testwiki"
GROUP BY event_username"""

edit_count_sql_template = u"""
SELECT  user_name, user_editcount
FROM    user
WHERE   user_name in (%s)
"""

headers = ["source", "uploads", "edits"]
def results_for(dg, sql, source):
    uploads = {}

    commons = dg.conn_for('commons')
    el = dg.conn_for('el')

    uploads_cursor = el.cursor()
    uploads_cursor.execute(dg.render(sql))

    for row in uploads_cursor:
        username = row[0].strip()
        username = username[0].upper() + username[1:]
        # Default Edit Count to 0. Happens only if username is not found in commons
        # Only case seems to be PhilK10, super early Commons iOS app tester who has
        # An account on testwiki, but not on Commons
        # The earliest version of the iOS app didn't differentiate between test and commons
        uploads[username] = [source, row[1], 0]

    edit_count_sql = edit_count_sql_template % ('%s,' * len(uploads.keys()))[:-1]
    edits_cursor = commons.cursor()
    edits_cursor.execute(edit_count_sql, uploads.keys())

    for row in edits_cursor:
        username = row[0].strip().decode('utf8')
        username = username[0].upper() + username[1:]
        uploads[username][-1] = row[1]
    return uploads.values()

def execute(dg):

    results = []
    results.extend(results_for(dg, mobile_uploaders_ios_sql, "iOS"))
    results.extend(results_for(dg, mobile_uploaders_android_sql, "Android"))
    # Commenting out, because web doesn't track username. UGH
    #results.extend(results_for(dg, mobile_uploaders_web_sql, "Web"))

    return (headers, results)
