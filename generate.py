import os
import sys
import glob
import MySQLdb as mysql
from jinja2 import Template
import yaml

#TODO: Have a subcommand that generates limnpy based data sources.
import limnpy

conn = mysql.connect(
    host=os.environ.get("STAT_HOST", "s1-analytics-slave.eqiad.wmnet"),
    port=int(os.environ.get("STAT_PORT", 3306)),
    user="research",
    passwd=os.environ["RESEARCH_PASSWORD"],
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
    #url_fmt = 'http://stat1001.wikimedia.org/mobile-dashbaord/%s'
    for key, sql in graphs.items():
        print "Generating %s" % key
        rows = execute(sql)
        headers = [field[0] for field in rows.description]
	name = config['graphs'][key]['title']
        ds = limnpy.DataSource(limn_id=key, limn_name=name, limn_group='mobile', data=list(rows), labels=headers, date_key='Date')
	ds.source['shortName'] = key # FIXME: Hack, since limn_name also sets shortName
        #url = url_fmt % key
        #ds = limnpy.DataSource(limn_id=key, limn_name=key, limn_group='mobile', data=list(rows), labels=headers, url=url, date_key='Date')
        ds.write(basedir='.')
        ds.write_graph(basedir='.')
