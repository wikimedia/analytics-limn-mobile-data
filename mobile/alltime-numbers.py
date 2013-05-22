uploads_count_android_sql = u"""
SELECT  count(*) AS uploads
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'Android%'
        AND event_result="success"
        AND wiki="commonswiki"
"""

uploads_count_ios_sql = u"""
SELECT  count(*) AS uploads
FROM    {{tables.upload_attempts}}
WHERE   event_platform LIKE 'iOS%'
        AND event_result="success"
        AND wiki="commonswiki"
"""

uploads_count_web_sql = u"""
SELECT  count(*) AS uploads
FROM    {{tables.upload_web}}
WHERE   event_action="success"
        AND wiki != "testwiki"
"""

retained_uploads_count_sql = u"""
SELECT  count(*)
FROM    categorylinks
WHERE   cl_to = %s
"""

headers = ["source", "total", "undeleted", "deletion percentage"]

# Picked up from result of error-correction.py
# These are recorded in EL as belonging on commons while they actually are in testwiki
modifiers = {
        "Android": 105,
        "iOS": 47,
        "Web": 0
        }


def results_for(dg, sql, source, category):
    commons = dg.get_connection('commons')
    el = dg.get_connection('el')

    all_cursor = el.cursor()
    all_cursor.execute(dg.render(sql))

    all_uploads = all_cursor.fetchone()

    existing_cursor = commons.cursor()
    existing_cursor.execute(retained_uploads_count_sql, category)

    existing_uploads = existing_cursor.fetchone()

    total_uploads = all_uploads[0] - modifiers[source]
    undeleted_uploads = existing_uploads[0]
    deleted_uploads = total_uploads - undeleted_uploads
    deletion_percentage = float(deleted_uploads) / float(total_uploads)

    return [source, total_uploads, undeleted_uploads, '%.2f' % deletion_percentage]


def execute(dg):

    results = []
    results.append(results_for(dg, uploads_count_android_sql, "Android", "Uploaded_with_Mobile/Android"))
    results.append(results_for(dg, uploads_count_ios_sql, "iOS", "Uploaded_with_Mobile/iOS"))
    results.append(results_for(dg, uploads_count_web_sql, "Web", "Uploaded_with_Mobile/Web"))

    return (headers, results)
