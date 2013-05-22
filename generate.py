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


class DataGenerator(object):
    """Executes queries and generates CSV reports based on YAML configs."""

    def __init__(self, folder, config_override=None):
        """Reads configuration 'config.yaml' in `folder_path`."""
        self.folder = folder
        self.config = {}
        self.connections = {}
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
            raise ValueError('No such database: "%s"' % name)

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

    def execute(self):
        """Generates a CSV report by executing Python code and SQL queries."""
        for key, value in self.config['graphs'].iteritems():
            # Look for the sql first, then python
            db_name = value.get('db', self.config['defaults']['db'])

            if os.path.exists(os.path.join(self.folder, key + '.sql')):
                file_path = os.path.join(self.folder, key + '.sql')
                headers, rows = self.execute_sql(file_path, db_name)
            elif os.path.exists(os.path.join(self.folder, key + '.py')):
                file_path = os.path.join(self.folder, key + '.py')
                headers, rows = self.execute_python(key, file_path)
            else:
                raise ValueError("Can not find SQL or Python for %s" % key)

            print "Generating %s (%s)" % (value['title'], file_path)

            output_path = self.config['output']['path']
            csv_filename = os.path.join(output_path, key + '.csv')

            with open(csv_filename, 'wb') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate data for the mobile dashboard.')
    parser.add_argument('folder', help='folder with config.yaml and *.sql files')
    parser.add_argument('-c', '--config-override', help='config.yaml override')
    args = parser.parse_args()

    dg = DataGenerator(**vars(args))
    dg.execute()
