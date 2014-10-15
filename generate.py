#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import imp
import io
import os
import sys
import argparse

import MySQLdb
import jinja2
import yaml

import time
import datetime
from dateutil.relativedelta import relativedelta
import json

from traceback import format_exc

LOG_FILE = 'history.json'


class DataGenerator(object):
    """Executes queries and generates CSV reports based on YAML configs."""

    def __init__(self, folder, debug_folder=None, config_override=None, graph=None, force=None):
        """Reads configuration 'config.yaml' in `folder_path`."""
        self.folder = folder
        self.debug_folder = debug_folder
        self.graph = graph
        self.config = {}
        self.connections = {}
        self.force = force
        config_main = os.path.join(folder, 'config.yaml')
        self.load_config(config_main)
        if config_override:
            self.load_config(config_override)

    def load_config(self, config_path):
        with io.open(config_path, encoding='utf-8') as config_file:
            self.config.update(yaml.load(config_file))

    def make_connection(self, name):
        """Opens a connection to a database using parameters specified in YAML
        file for a given name."""
        try:
            db = self.config['databases'][name]
        except KeyError:
            raise ValueError('No such database: "{0}"'.format(name))
        self.connections[name] = MySQLdb.connect(
            host=db['host'],
            port=db['port'],
            read_default_file=db['creds_file'],
            db=db['db'],
            charset='utf8',
            use_unicode=True
        )

    def get_connection(self, name):
        """Gets a database connection, specified by its name in the
        configuration files. If no connection exists, makes it."""
        if name not in self.connections:
            self.make_connection(name)
        return self.connections[name]

    def render(self, template):
        """Constructs a SQL query string by interpolating values into a jinja
        template."""
        t = jinja2.Template(template)
        return t.render(**self.config)

    def get_sql_string(self, file_name):
        with io.open(file_name, encoding='utf-8') as f:
            return self.render(f.read())

    def execute_sql(self, sql, db_name, graph_key):
        """Executes a query `sql` against
        a database `db_name`

        Returns a tuple of (headers, rows).
        """

        if self.debug_folder:
            debug_filename = os.path.join(self.debug_folder, graph_key + '.sql')
            with open(debug_filename, 'wb') as debug_file:
                debug_file.write(sql)

        conn = self.get_connection(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            headers = [field[0] for field in cursor.description]
        except Exception, e:
            print "Issue executing SQL for %s (%s)" % (graph_key, e)
            headers = []
            rows = []
        finally:
            cursor.close()
        return (headers, rows)

    def execute_python(self, name, file_path, graph_key):
        """Does unspeakable evil. Look away!"""
        module = imp.load_source(name, file_path)
        try:
            res = module.execute(self)
            return res
        except Exception, e:
            print "Issue executing Python script for %s (%s)" % (graph_key, e)
            return ([], [])

    def save_history(self, data):
        dump = json.dumps(data)
        f = open(LOG_FILE, 'w')
        f.writelines(dump)
        f.close()

    def get_history(self):
        try:
            if not os.path.exists(LOG_FILE):
                f = open(LOG_FILE, 'w')
                f.write('{}')
                f.close()
            f = open(LOG_FILE, 'r')
            data = '\n'.join(f.readlines())
            f.close()
            try:
                return json.loads(data)
            except ValueError:
                print('invalid JSON - someone tweaked the history file!')
                return {}
        except IOError:
            return {}

    def execute(self):
        history = self.get_history()
        """Generates a CSV report by executing Python code and SQL queries."""
        if self.graph:
            name = self.graph
            graphs = {name: self.config['graphs'][name]}
        else:
            graphs = self.config['graphs']

        for key, value in graphs.iteritems():
            # title = value['title']
            freq = value['frequency']
            try:
                last_run_time = history[key]
            except KeyError:
                last_run_time = 0

            now = time.time()
            if freq == 'daily':
                increment = 60 * 60 * 24
            elif freq == 'hourly':
                increment = 60 * 60
            else:
                increment = 0
            due_at = last_run_time + increment

            if due_at < now or self.force:
                print('Generating {0}'.format(value['title']))
                if "timeboxed" in value and "starts" in value:
                    from_date = value["starts"]

                    if "ends" in value:
                        to_date = value["ends"]
                    else:
                        to_date = None
                    ok = self.generate_graph_timeboxed(key, value, from_date, to_date)
                else:
                    ok = self.generate_graph_full(key, value)

                if ok:
                    try:
                        history[key] = now
                    except:
                        continue
                    finally:
                        if history[key] == now:
                            self.save_history(history)
            else:
                print('Skipping generation of {0}: not enough time has passed'.format(value['title']))

    def generate_graph_timeboxed(self, graph_key, value, from_date, to_date=None):
        csv_filename = self.get_csv_filename(graph_key)
        cache = {}
        # load existing values from csv
        csv_header = None
        try:
            with open(csv_filename, 'r') as csv_file:
                reader = csv.reader(csv_file)
                for row in reader:
                    # skip header
                    if csv_header:
                        # store in cache only if the row is full or something other than null values
                        if not all([not a for a in row[1:]]):
                            cache[row[0]] = row[1:]
                    else:
                        csv_header = row
        except IOError:
            pass

        if value['frequency'] == 'hourly':
            is_daily_graph = True
            date_type = 'Day'
        else:
            is_daily_graph = False
            date_type = 'Month'

        today = datetime.date.today()
        this_month = datetime.date(today.year, today.month, 1)
        if to_date:
            end_date = to_date
        else:
            if is_daily_graph:
                end_date = datetime.date(today.year, today.month, today.day)
            else:
                end_date = this_month

        sql_path = self.get_sql_path(graph_key)

        if os.path.exists(sql_path):
            while from_date <= end_date:
                graph_date_key = from_date.strftime('%Y-%m-%d')
                from_timestamp = from_date.strftime('%Y%m%d%H%M%S')
                if is_daily_graph:
                    from_date = from_date + relativedelta(days=1)
                else:
                    from_date = from_date + relativedelta(months=1)
                to_timestamp = from_date.strftime('%Y%m%d%H%M%S')
                # Generate a graph if not in cache or the current month
                if graph_date_key not in cache or from_date >= this_month:
                    print 'Generating data for timestamp %s to %s' % (from_timestamp, to_timestamp)
                    db_name = value.get('db', self.config['defaults']['db'])
                    query = self.get_sql_string(sql_path)
                    # apply timeboxing
                    query = query.format(
                        from_timestamp=from_timestamp,
                        to_timestamp=to_timestamp
                    )
                    headers, rows = self.execute_sql(query, db_name, graph_key)
                    if headers and csv_header:
                        if len(headers) + 1 != len(csv_header) and not self.force:
                            print "Data format has changed. Aborting."
                            self.clear_csv_data(graph_key)
                            self.generate_graph_timeboxed(graph_key, value, from_date, to_date)
                            return
                    if not csv_header:
                        csv_header = headers
                        # FIXME: Support other time periods other than months?
                        csv_header.insert(0, date_type)
                    try:
                        cache[graph_date_key] = list(rows[0])
                    except Exception, e:
                        print('{}: Error: {}, db: {}, graph key: {}, rows: {}, query: \n{}'.format(
                            datetime.datetime.now(), format_exc(e), db_name, graph_key, rows, query
                        ))
                        # stop the loop because one exception is likely to mean total fail
                        break
                else:
                    print 'Skip generation of %s' % graph_date_key

            rows = []
            for month, row in iter(sorted(cache.iteritems())):
                row.insert(0, month)
                rows.append(row)
            if len(rows) > 0:
                self.save_graph_as_csv(graph_key, csv_header, rows)
            else:
                print 'No data to write.'
        else:
            print 'Bad SQL given'

    def generate_graph_full(self, key, value):
        """
        Runs python or sql to generate csv.  Returns:
          True  - something was generated
          False - nothing was generated
        """
        # Look for the sql first, then python
        db_name = value.get('db', self.config['defaults']['db'])

        sql_path = self.get_sql_path(key)
        if os.path.exists(sql_path):
            query = self.get_sql_string(sql_path)
            headers, rows = self.execute_sql(query, db_name, key)
        elif os.path.exists(os.path.join(self.folder, key + '.py')):
            file_path = os.path.join(self.folder, key + '.py')
            headers, rows = self.execute_python(key, file_path, key)
        else:
            raise ValueError('Can not find SQL or Python for {0}'.format(key))
        self.save_graph_as_csv(key, headers, rows)
        return not(headers is None or rows is None or len(headers) == 0 or len(rows) == 0)

    def get_sql_path(self, key):
        return os.path.join(self.folder, key + '.sql')

    def get_csv_filename(self, key):
        output_path = self.config['output']['path']
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        return os.path.join(output_path, key + '.csv')

    def clear_csv_data(self, key):
        filename = self.get_csv_filename(key)
        return os.remove(filename)

    def save_graph_as_csv(self, key, headers, rows):
            csv_filename = self.get_csv_filename(key)
            with open(csv_filename, 'wb') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate data for the mobile dashboard.')
    parser.add_argument('folder', help='folder with config.yaml and *.sql files')
    parser.add_argument('-c', '--config-override', help='config.yaml override')
    parser.add_argument('-d', '--debug-folder', help='save generated SQL in a given folder')
    parser.add_argument('-g', '--graph', help='the name of a single graph you want to generate for')
    parser.add_argument('-f', '--force', dest='force', action='store_true',
                        help='Force generation of graph regardless of when last generated')
    parser.set_defaults(force=False)
    args = parser.parse_args()

    dg = DataGenerator(**vars(args))
    dg.execute()
