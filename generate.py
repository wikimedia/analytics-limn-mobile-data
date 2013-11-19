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
import json

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

    def execute_sql(self, file_name, db_name):
        """Reads a query from `file_name`, renders it, and executes it against
        a database, specified by configuration key.

        Returns a tuple of (headers, rows).
        """
        with io.open(file_name, encoding='utf-8') as f:
            sql = self.render(f.read())

        if self.debug_folder:
            debug_filename = os.path.join(self.debug_folder, os.path.basename(file_name))
            with open(debug_filename, 'wb') as debug_file:
                debug_file.write(sql)

        conn = self.get_connection(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            headers = [field[0] for field in cursor.description]
        finally:
            cursor.close()
        return (headers, rows)

    def execute_python(self, name, file_path):
        """Does unspeakable evil. Look away!"""
        module = imp.load_source(name, file_path)
        return module.execute(self)

    def save_history(self, data):
        dump = json.dumps(data)
        f = open(LOG_FILE, 'w')
        f.writelines(dump)
        f.close()

    def get_history(self):
        try:
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
                try:
                    self.generate_graph(key, value)
                    history[key] = now
                except:
                    continue
                finally:
                    if history[key] == now:
                        self.save_history(history)
            else:
                print('Skipping generation of {0}: not enough time has passed'.format(value['title']))

    def generate_graph(self, key, value):
            print('Generating {0}'.format(value['title']))
            # Look for the sql first, then python
            db_name = value.get('db', self.config['defaults']['db'])

            if os.path.exists(os.path.join(self.folder, key + '.sql')):
                file_path = os.path.join(self.folder, key + '.sql')
                headers, rows = self.execute_sql(file_path, db_name)
            elif os.path.exists(os.path.join(self.folder, key + '.py')):
                file_path = os.path.join(self.folder, key + '.py')
                headers, rows = self.execute_python(key, file_path)
            else:
                raise ValueError('Can not find SQL or Python for {0}'.format(key))

            output_path = self.config['output']['path']
            csv_filename = os.path.join(output_path, key + '.csv')

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
    parser.add_argument('-f', '--force', help='Force generation of graph regardless of when last generated')
    args = parser.parse_args()

    dg = DataGenerator(**vars(args))
    dg.execute()
