
# This module implements the first step in the pipeline.
# Reads the report information from the config file.
# For each report section contained in the config,
# creates a Report object, that will be passed
# to the rest of the pipeline.
#
# This step tries to check all possible input type and format
# issues to minimize the impact of a possible config error.


import os
import io
import logging
from datetime import datetime, date
from report import Report
from utils import DATE_FORMAT, raise_critical


class Reader(object):


    def __init__(self, config):
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.config = config


    def run(self):
        if 'reportupdater-reports' not in self.config:
            raise_critical(KeyError, 'Reportupdater-reports is not in config.')
        reports = self.config['reportupdater-reports']
        if not isinstance(reports, dict):
            raise_critical(ValueError, 'Reportupdater-reports is not a dict.')
        for report_key, report_config in reports.iteritems():
            logging.debug('Reading "{report_key}"...'.format(report_key=report_key))
            try:
                report = self.create_report(report_key, report_config)
                yield report
            except Exception, e:
                message = ('Report "{report_key}" could not be read from config '
                           'because of error: {error}')
                logging.error(message.format(report_key=report_key, error=str(e)))


    def create_report(self, report_key, report_config):
        if not isinstance(report_key, str):
            raise TypeError('Report key is not a string.')
        if not isinstance(report_config, dict):
            raise TypeError('Report config is not a dict.')
        report = Report()
        report.key = report_key
        report.frequency, report.granularity = self.get_frequency_and_granularity(report_config)
        report.is_timeboxed = self.get_is_timeboxed(report_config)
        report.is_funnel = self.get_is_funnel(report_config)
        report.first_date = self.get_first_date(report_config, report.is_timeboxed)
        report.db_key = self.get_db_key()
        report.sql_template = self.get_sql_template(report_key)
        return report


    def get_frequency_and_granularity(self, report_config):
        if 'frequency' not in report_config:
            raise KeyError('Report frequency is not specified.')
        if report_config['frequency'] == 'hourly':
            return 'hours', 'days'
        elif report_config['frequency'] == 'daily':
            return 'days', 'months'
        else:
            raise ValueError('Report frequency is not valid.')


    def get_is_timeboxed(self, report_config):
        return 'timeboxed' in report_config and report_config['timeboxed'] is True


    def get_is_funnel(self, report_config):
        return 'funnel' in report_config and report_config['funnel'] is True


    def get_first_date(self, report_config, is_timeboxed):
        if 'starts' in report_config:
            first_date = report_config['starts']
            if isinstance(first_date, date):
                first_date = datetime(first_date.year, first_date.month, first_date.day)
            else:
                try:
                    first_date = datetime.strptime(first_date, DATE_FORMAT)
                except TypeError:
                    raise TypeError('Report starts is not a string.')
                except ValueError:
                    raise ValueError('Report starts does not match date format')
            return first_date
        elif is_timeboxed:
            raise ValueError('Timeboxed report does not specify starts.')
        else:
            return None


    def get_db_key(self):
        if 'defaults' not in self.config:
            raise KeyError('Defaults is not in config.')
        if 'db' not in self.config['defaults']:
            raise KeyError('DB default is not in defaults config.')
        db_key = self.config['defaults']['db']
        if not isinstance(db_key, str):
            raise ValueError('DB default is not a string.')
        return self.config['defaults']['db']


    def get_sql_template(self, report_key):
        if 'sql_folder' not in self.config:
            raise KeyError('SQL folder is not in config.')
        sql_folder = self.config['sql_folder']
        if not isinstance(sql_folder, str):
            raise ValueError('SQL folder is not a string.')
        sql_template_path = os.path.join(sql_folder, report_key + '.sql')
        try:
            with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
                return sql_template_file.read()
        except IOError, e:
            raise IOError('Could not read the SQL template (' + str(e) + ').')
