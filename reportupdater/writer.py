
# This module is the last step of the pipeline.
# It gets the results passed from the executor,
# and updates the report's corresponding file.
#
# In the case of timeboxed reports, it handles the
# update of previous results consistently.


import os
import io
import csv
import logging
from executor import Executor
from utils import (raise_critical, get_previous_results,
                   DATE_FORMAT, get_exploded_report_output_path)


class Writer(object):


    def __init__(self, executor, config):
        if not isinstance(executor, Executor):
            raise_critical(ValueError, 'Executor is not valid.')
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.executor = executor
        self.config = config


    def run(self):
        if 'output_folder' not in self.config:
            raise KeyError('Output folder is not in config.')
        output_folder = self.config['output_folder']
        if not isinstance(output_folder, str):
            raise ValueError('Output folder is not a string.')

        for report in self.executor.run():
            logging.debug('Writing "{report}"...'.format(report=str(report)))
            previous_results = get_previous_results(report, output_folder)
            previous_header = previous_results['header']
            header = report.results['header']
            if len(previous_header) > 0 and header != previous_header:
                raise ValueError('Results header does not match previous headers.')

            updated_data = previous_results['data']
            for date, rows in report.results['data'].iteritems():
                updated_data[date] = rows
            try:
                self.write_results(header, updated_data, report, output_folder)
                logging.info('Report {report_key} has been updated.'.format(report_key=report.key))
            except Exception, e:
                message = ('Report "{report_key}" could not be written '
                           'because of error: {error}')
                logging.error(message.format(report_key=report.key, error=str(e)))


    def write_results(self, header, data, report, output_folder):
        dates = sorted(data.keys())
        rows = [data[date] for date in dates]
        if report.is_funnel:
            rows = [row for sublist in rows for row in sublist]  # flatten
        if len(report.explode_by) > 0:
            output_path = get_exploded_report_output_path(
                self.config['output_folder'], report.explode_by, report.key)
        else:
            output_path = os.path.join(self.config['output_folder'], report.key + '.tsv')
        temp_output_path = output_path + '.tmp'

        try:
            # wb mode needed to avoid unicode conflict between io and csv
            temp_output_file = io.open(temp_output_path, 'wb')
        except Exception, e:
            raise RuntimeError('Could not open the temporary output file (' + str(e) + ').')
        tsv_writer = csv.writer(temp_output_file, delimiter='\t')
        tsv_writer.writerow(header)
        for row in rows:
            row[0] = row[0].strftime(DATE_FORMAT)
            tsv_writer.writerow(row)
        temp_output_file.close()
        try:
            os.rename(temp_output_path, output_path)
        except Exception, e:
            raise RuntimeError('Could not rename the output file (' + str(e) + ').')
