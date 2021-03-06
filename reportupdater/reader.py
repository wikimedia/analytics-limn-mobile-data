
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
from utils import DATE_FORMAT, raise_critical, get_wikis


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
        if 'query_folder' not in self.config:
            raise KeyError('Query folder is not in config.')
        query_folder = self.config['query_folder']
        if not isinstance(query_folder, str):
            raise ValueError('Query folder is not a string.')
        report = Report()
        report.key = report_key
        report.type = self.get_type(report_config)
        report.frequency = self.get_frequency(report_config)
        report.granularity = self.get_granularity(report_config)
        report.lag = self.get_lag(report_config)
        report.is_timeboxed = self.get_is_timeboxed(report_config)
        report.is_funnel = self.get_is_funnel(report_config)
        report.first_date = self.get_first_date(report_config, report.is_timeboxed)
        report.explode_by = self.get_explode_by(report_config)
        if report.type == 'sql':
            report.db_key = self.get_db_key(report_config)
            report.sql_template = self.get_sql_template(report_key, query_folder)
        elif report.type == 'script':
            report.script = self.get_script(report_key, query_folder)
        return report


    def get_type(self, report_config):
        report_type = report_config.get('type', 'sql')
        if report_type not in ['sql', 'script']:
            raise ValueError('Report type is not valid.')
        return report_type


    def get_frequency(self, report_config):
        if 'frequency' not in report_config:
            raise KeyError('Report frequency is not specified.')
        frequency = report_config['frequency']
        if frequency not in ['hours', 'days', 'weeks', 'months']:
            raise ValueError('Report frequency is not valid.')
        return frequency


    def get_granularity(self, report_config):
        if 'granularity' not in report_config:
            raise KeyError('Report granularity is not specified.')
        granularity = report_config['granularity']
        if granularity not in ['days', 'weeks', 'months']:
            raise ValueError('Report granularity is not valid.')
        return granularity


    def get_lag(self, report_config):
        if 'lag' not in report_config:
            return 0
        lag = report_config['lag']
        if type(lag) != int or lag < 0:
            raise ValueError('Report lag is not valid.')
        return lag


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


    def get_db_key(self, report_config):
        if 'db' in report_config:
            db_key = report_config['db']
        elif 'defaults' not in self.config:
            raise KeyError('Defaults is not in config.')
        elif 'db' not in self.config['defaults']:
            raise KeyError('DB default is not in defaults config.')
        else:
            db_key = self.config['defaults']['db']
        if not isinstance(db_key, str):
            raise ValueError('DB key is not a string.')
        return db_key


    def get_sql_template(self, report_key, query_folder):
        sql_template_path = os.path.join(query_folder, report_key + '.sql')
        try:
            with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
                return sql_template_file.read()
        except IOError, e:
            raise IOError('Could not read the SQL template (' + str(e) + ').')


    def get_script(self, report_key, query_folder):
        return os.path.join(query_folder, report_key)


    def get_explode_by(self, report_config):
        explode_by = {}
        if 'by_wiki' in report_config and report_config['by_wiki'] is True:
            explode_by['wiki'] = get_wikis(self.config) + ['all']
        if 'explode_by' in report_config:
            for placeholder, values_str in report_config['explode_by'].iteritems():
                values = [value.strip() for value in values_str.split(',')]
                explode_by[placeholder] = values
        return explode_by
