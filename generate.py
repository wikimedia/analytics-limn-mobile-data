import os
import MySQLdb as mysql
from jinja2 import Template
#import limnpy
import unicodecsv as csv
import yaml

conn = mysql.connect("s1-analytics-slave.eqiad.wmnet", "research", os.environ["RESEARCH_PASSWORD"], "log")
#conn = mysql.connect("s1-analytics-slave.eqiad.wmnet", read_default_file=os.path.expanduser('~/.my.cnf.research'), db="log")

def execute(sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur

def render(template, env):
    t = Template(template)
    return t.render(**env)

graphs = {
        "error-uploads": {
            "sql": """SELECT    DATE(timestamp) as Date,
                                COUNT( * ) as Total,

                                SUM( IF( event_platform LIKE 'Android%', 1, 0) ) AS Android,
                                SUM( IF( event_platform LIKE 'iOS%', 1, 0) ) AS iOS

                      FROM      {{ tables.upload_attempts }}
                      WHERE     event_result != 'cancelled' AND
                                event_result != 'success' AND
                                wiki = 'commonswiki'
                      GROUP BY  DATE(timestamp)
                      """
                      },
        "cancelled-uploads": {
            "sql": """SELECT    DATE(timestamp) as Date,
                                COUNT( * ) as Total,

                                SUM( IF( event_platform LIKE 'Android%', 1, 0) ) AS Android,
                                SUM( IF( event_platform LIKE 'iOS%', 1, 0) ) AS iOS
                      FROM      {{ tables.upload_attempts }}
                      WHERE     event_result = 'cancelled' AND
                                wiki = 'commonswiki'
                      GROUP BY  DATE(timestamp)
                      """
                      },
        "successful-uploads": {
            "sql": """SELECT    DATE(timestamp) as Date,
                                COUNT( * ) as Total,

                                SUM( IF( event_platform LIKE 'Android%', 1, 0) ) AS Android,
                                SUM( IF( event_platform LIKE 'iOS%', 1, 0) ) AS iOS

                      FROM      {{ tables.upload_attempts }}
                      WHERE     event_result = 'success' AND
                                wiki = 'commonswiki'
                      GROUP BY  DATE(timestamp)
                      """
                      },
        "unique-uploaders": {
            "sql": """SELECT    DATE(T1.timestamp),
                                (SELECT COUNT( DISTINCT T2.event_username ) 
                                    FROM {{ tables.upload_attempts }} AS T2
                                    WHERE   T2.timestamp < T1.timestamp AND
                                            (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average }} )) AS Total,
                                (SELECT COUNT( DISTINCT T2.event_username ) 
                                    FROM {{ tables.upload_attempts }} AS T2
                                    WHERE   T2.event_platform LIKE 'Android%' AND
                                            T2.timestamp < T1.timestamp AND
                                            (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average }} )) AS Android,
                                (SELECT COUNT( DISTINCT T2.event_username ) 
                                    FROM {{ tables.upload_attempts }} AS T2
                                    WHERE   T2.event_platform LIKE 'iOS%' AND
                                            T2.timestamp < T1.timestamp AND
                                            (T2.timestamp + INTERVAL 0 DAY) > (T1.timestamp - INTERVAL {{ intervals.running_average}} )) AS iOS
                      FROM      {{ tables.upload_attempts }} AS T1
                      WHERE     T1.event_result = 'success' AND
                                T1.wiki = 'commonswiki'
                      GROUP BY  DATE(T1.timestamp)
                      """
                      },
        "successful-logins": {
            "sql": """SELECT    DATE(timestamp) as Date,
                                COUNT( * ) as Total, 
                                SUM( IF( event_platform LIKE 'Android%', 1, 0) ) AS Android,
                                SUM( IF( event_platform LIKE 'iOS%', 1, 0) ) AS iOS
                      FROM      {{ tables.login_attempts }}
                      WHERE     event_result = 'success' AND
                                wiki = 'commonswiki'
                      GROUP BY  DATE(timestamp)
                      """
                      }
        }


if __name__ == "__main__":
    sql_env = yaml.load(open("generator/mobile-app-stats.yaml"))
    for key, value in graphs.items():
        sql = render(value['sql'], sql_env)
        rows = execute(sql)
        headers = [field[0] for field in rows.description]
        #ds = limnpy.DataSource(limn_id=key, limn_name=key, data=list(rows), labels=value['headers'], date_key='Date')
        #ds.write(basedir='.')
        #ds.write_graph(basedir='.')
        writer = csv.writer(open("datafiles/" + key + ".csv", "w"))
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
