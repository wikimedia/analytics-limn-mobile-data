
# This module implements the report object that serves as
# communication unit between the several pipeline layers.
# It holds all the information referent to a report,
# such as granularity, start and end dates and results.
# It is not intended to hold any logic.


import re
from datetime import datetime
from utils import DATE_FORMAT


class Report(object):


    def __init__(self):
        self.key = None
        self.frequency = None
        self.granularity = None
        self.is_timeboxed = False
        self.is_funnel = False
        self.first_date = None
        self.start = None
        self.end = None
        self.db_key = None
        self.sql_template = None
        self.explode_by = {}
        self.results = {'header': [], 'data': {}}


    def __str__(self):
        return (
            '<Report' +
            ' key=' + str(self.key) +
            ' frequency=' + str(self.frequency) +
            ' granularity=' + str(self.granularity) +
            ' is_timeboxed=' + str(self.is_timeboxed) +
            ' is_funnel=' + str(self.is_funnel) +
            ' first_date=' + self.format_date(self.first_date) +
            ' start=' + self.format_date(self.start) +
            ' end=' + self.format_date(self.end) +
            ' db_key=' + str(self.db_key) +
            ' sql_template=' + self.format_sql(self.sql_template) +
            ' explode_by=' + str(self.explode_by) +
            ' results=' + self.format_results(self.results) +
            '>'
        )


    def format_date(self, to_format):
        if to_format:
            if isinstance(to_format, datetime):
                return to_format.strftime(DATE_FORMAT)
            else:
                return 'invalid date'
        else:
            return str(None)


    def format_results(self, to_format):
        if not isinstance(to_format, dict):
            return 'invalid results'
        header = str(to_format.get('header', 'invalid header'))
        data = to_format.get('data', None)
        if isinstance(data, dict):
            data_lines = str(len(data)) + ' rows'
        else:
            data_lines = 'invalid data'
        return str({'header': header, 'data': data_lines})


    def format_sql(self, to_format):
        if to_format is None:
            return str(None)
        sql = re.sub(r'\s+', ' ', to_format).strip()
        if len(sql) > 100:
            sql = sql[0:100] + '...'
        return sql
