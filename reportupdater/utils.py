
# This is a helper file that contains various utils.
# Date formatters, logging facilities and a result parser.


import os
import io
import csv
import logging
from datetime import datetime
from collections import defaultdict


DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
DATE_AND_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def raise_critical(error_class, message):
    logging.critical(message)
    raise error_class(message)


def get_previous_results(report, output_folder):
    # Reads a report file to get its results
    # and returns them in the expected dict(date->row) format.
    previous_results = {'header': [], 'data': {}}
    output_file_path = os.path.join(output_folder, report.key + '.csv')
    if os.path.exists(output_file_path):
        try:
            with io.open(output_file_path, encoding='utf-8') as output_file:
                rows = list(csv.reader(output_file))
        except IOError, e:
            raise IOError('Could not read the output file (' + str(e) + ').')
        header = []
        if report.is_funnel:
            # If the report is for a funnel visualization,
            # one same date may contain several lines in the csv.
            # So, all lines for the same date, are listed in the
            # same dict entry under the date key.
            data = defaultdict(list)
        else:
            data = {}
        for row in rows:
            if not header:
                header = row  # skip header
            else:
                try:
                    date = datetime.strptime(row[0], DATE_FORMAT)
                except ValueError:
                    raise ValueError('Output file date does not match date format.')
                row[0] = date
                if report.is_funnel:
                    data[date].append(row)
                else:
                    data[date] = row
        previous_results['header'] = header
        previous_results['data'] = data
    return previous_results
