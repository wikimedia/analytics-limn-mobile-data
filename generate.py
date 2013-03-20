import os
import sys
import glob
import MySQLdb as mysql
from jinja2 import Template
import yaml
import csv

conn = mysql.connect(
    host=os.environ.get("STAT_HOST", "s1-analytics-slave.eqiad.wmnet"),
    port=int(os.environ.get("STAT_PORT", 3306)),
    read_default_file='/a/.my.cnf.research',
    db="log"
)
#conn = mysql.connect("s1-analytics-slave.eqiad.wmnet", read_default_file=os.path.expanduser('~/.my.cnf.research'), db="log")

def execute(sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur

def render(template, env):
    t = Template(template)
    return t.render(**env)

if __name__ == "__main__":
    if len(sys.argv) != 2: #FIXME: argparse please
        print "Usage: generate.py <folder with config.yaml and *.sql files>"
        sys.exit(-1)

    folder = sys.argv[1]
    config = yaml.load(open(os.path.join(folder, "config.yaml")))
    graphs = dict([
        (   os.path.basename(filename).split(".")[0], 
            render(open(filename).read(), config)
        ) for filename in glob.glob(os.path.join(folder, "*.sql"))
    ])
    #url_fmt = 'http://stat1001.wikimedia.org/mobile-dashboard/%s'
    for key, sql in graphs.items():
        print "Generating %s" % key
        cursor = execute(sql)
        rows = cursor.fetchall()
        
        csvOutput = open(os.path.join('/a/limn-public-data/mobile-apps', key + '.csv'), 'w')
        csvOutputWriter = csv.writer(csvOutput)

        headers = [field[0] for field in cursor.description]
        csvOutputWriter.writerow(headers)
        csvOutputWriter.writerows(rows)

        csvOutput.close()
