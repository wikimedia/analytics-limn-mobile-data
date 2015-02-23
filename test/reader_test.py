
import os
import io
from reportupdater.reader import Reader
from reportupdater.utils import DATE_FORMAT
from unittest import TestCase
from mock import MagicMock
from datetime import datetime


class ReaderTest(TestCase):


    def setUp(self):
        self.report_key = 'reader_test'
        self.report_config = {
            'starts': '2015-01-01',
            'timeboxed': True,
            'frequency': 'hourly'
        }
        self.config = {
            'sql_folder': 'test/fixtures/sql',
            'output_folder': 'test/fixtures/output',
            'reportupdater-reports': {
                self.report_key: self.report_config
            },
            'defaults': {
                'db': 'db_key'
            }
        }
        self.reader = Reader(self.config)


    def test_get_frequency_and_granularity_when_report_frequency_is_not_in_config(self):
        report_config = {}
        with self.assertRaises(KeyError):
            self.reader.get_frequency_and_granularity(report_config)


    def test_get_frequency_and_granularity_when_report_frequency_is_not_hourly_or_daily(self):
        report_config = {'frequency': 'wrongly'}
        with self.assertRaises(ValueError):
            self.reader.get_frequency_and_granularity(report_config)


    def test_get_frequency(self):
        corresponding = {
            'hourly': ('hours', 'days'),
            'daily': ('days', 'months')
        }
        for frequency, expected in corresponding.iteritems():
            report_config = {'frequency': frequency}
            result = self.reader.get_frequency_and_granularity(report_config)
            self.assertEqual(result, expected)


    def test_get_is_timeboxed_when_report_timeboxed_is_not_in_config(self):
        report_config = {}
        is_timeboxed = self.reader.get_is_timeboxed(report_config)
        self.assertFalse(is_timeboxed)


    def test_get_is_timeboxed_when_report_timeboxed_is_not_true(self):
        for value in [False, None, 0]:
            report_config = {'timeboxed': value}
            is_timeboxed = self.reader.get_is_timeboxed(report_config)
            self.assertFalse(is_timeboxed)


    def test_get_is_timeboxed_when_report_timeboxed_is_true(self):
        report_config = {'timeboxed': True}
        is_timeboxed = self.reader.get_is_timeboxed(report_config)
        self.assertTrue(is_timeboxed)


    def test_get_is_funnel_when_report_funnel_is_not_in_config(self):
        report_config = {}
        is_funnel = self.reader.get_is_funnel(report_config)
        self.assertFalse(is_funnel)


    def test_get_is_funnel_when_report_funnel_is_not_true(self):
        for value in [False, None, 0]:
            report_config = {'funnel': value}
            is_funnel = self.reader.get_is_funnel(report_config)
            self.assertFalse(is_funnel)


    def test_get_is_funnel_when_report_funnel_is_true(self):
        report_config = {'funnel': True}
        is_funnel = self.reader.get_is_funnel(report_config)
        self.assertTrue(is_funnel)


    def test_get_first_date_when_report_starts_is_not_a_string(self):
        report_config = {'starts': ('not', 'a', 'string')}
        is_timeboxed = True
        with self.assertRaises(TypeError):
            self.reader.get_first_date(report_config, is_timeboxed)


    def test_get_first_date_when_report_starts_does_not_match_date_format(self):
        report_config = {'starts': 'no match'}
        is_timeboxed = True
        with self.assertRaises(ValueError):
            self.reader.get_first_date(report_config, is_timeboxed)


    def test_get_first_date_when_report_starts_is_not_in_timeboxed_config(self):
        report_config = {}
        is_timeboxed = True
        with self.assertRaises(ValueError):
            self.reader.get_first_date(report_config, is_timeboxed)


    def test_get_first_date_when_report_starts_is_not_in_config(self):
        report_config = {}
        is_timeboxed = False
        first_date = self.reader.get_first_date(report_config, is_timeboxed)
        self.assertEqual(first_date, None)


    def test_get_first_date(self):
        date_str = '2015-01-01'
        report_config = {'starts': date_str}
        is_timeboxed = True
        result = self.reader.get_first_date(report_config, is_timeboxed)
        expected = datetime.strptime(date_str, DATE_FORMAT)
        self.assertEqual(result, expected)


    def test_get_db_key_when_defaults_is_not_in_config(self):
        reader = Reader({})
        with self.assertRaises(KeyError):
            reader.get_db_key()


    def test_get_db_key_when_defaults_db_is_not_in_config(self):
        config = {'defaults': {}}
        reader = Reader(config)
        with self.assertRaises(KeyError):
            reader.get_db_key()


    def test_get_db_key_when_defaults_db_is_not_a_string(self):
        config = {
            'defaults': {
                'db': None
            }
        }
        reader = Reader(config)
        with self.assertRaises(ValueError):
            reader.get_db_key()


    def test_get_db_key(self):
        result = self.reader.get_db_key()
        expected = self.config['defaults']['db']
        self.assertEqual(result, expected)


    def test_get_sql_template_when_sql_folder_is_not_in_config(self):
        reader = Reader({})
        with self.assertRaises(KeyError):
            reader.get_sql_template('reader_test')


    def test_get_sql_template_when_sql_folder_is_not_a_string(self):
        config = {'sql_folder': ('not', 'a', 'string')}
        reader = Reader(config)
        with self.assertRaises(ValueError):
            reader.get_sql_template('reader_test')


    def test_get_sql_template_when_sql_folder_does_not_exist(self):
        config = {'sql_folder': 'nonexistent'}
        reader = Reader(config)
        with self.assertRaises(IOError):
            reader.get_sql_template('reader_test')


    def test_get_sql_template_when_sql_file_does_not_exist(self):
        with self.assertRaises(IOError):
            self.reader.get_sql_template('wrong_report_key')


    def test_get_sql_template(self):
        result = self.reader.get_sql_template('reader_test')
        sql_folder = self.config['sql_folder']
        report_key = 'reader_test'
        sql_template_path = os.path.join(sql_folder, report_key + '.sql')
        with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
            expected = sql_template_file.read()
        self.assertEqual(result, expected)


    def test_create_report_when_report_key_is_not_a_string(self):
        report_key = ('not', 'a', 'string')
        with self.assertRaises(TypeError):
            self.reader.create_report(report_key, self.report_config)


    def test_create_report_when_report_config_is_not_a_dict(self):
        report_config = None
        with self.assertRaises(TypeError):
            self.reader.create_report(self.report_key, report_config)


    def test_create_report_when_helper_method_raises_error(self):
        self.reader.get_first_date = MagicMock(side_effect=Exception())
        with self.assertRaises(Exception):
            self.reader.create_report(self.report_key, self.report_config)


    def test_create_report(self):
        self.reader.get_first_date = MagicMock(return_value='first_date')
        self.reader.get_frequency_and_granularity = MagicMock(
            return_value=('frequency', 'granularity'))
        self.reader.get_is_timeboxed = MagicMock(return_value='is_timeboxed')
        self.reader.get_is_funnel = MagicMock(return_value='is_funnel')
        self.reader.get_db_key = MagicMock(return_value='db_key')
        self.reader.get_sql_template = MagicMock(return_value='sql_template')
        report = self.reader.create_report(self.report_key, self.report_config)
        self.assertEqual(report.key, self.report_key)
        self.assertEqual(report.first_date, 'first_date')
        self.assertEqual(report.frequency, 'frequency')
        self.assertEqual(report.granularity, 'granularity')
        self.assertEqual(report.is_timeboxed, 'is_timeboxed')
        self.assertEqual(report.is_funnel, 'is_funnel')
        self.assertEqual(report.db_key, 'db_key')
        self.assertEqual(report.sql_template, 'sql_template')
        self.assertEqual(report.results, {'header': [], 'data': {}})
        self.assertEqual(report.start, None)
        self.assertEqual(report.end, None)


    def test_run_when_reports_is_not_in_config(self):
        reader = Reader({})
        with self.assertRaises(KeyError):
            list(reader.run())


    def test_run_when_reports_is_not_a_dict(self):
        config = {'reportupdater-reports': ('not', 'a', 'dict')}
        reader = Reader(config)
        with self.assertRaises(ValueError):
            list(reader.run())


    def test_run_when_create_report_raises_error(self):
        self.reader.create_report = MagicMock(side_effect=Exception())
        for report in self.reader.run():
            self.assertTrue(False)


    def test_run(self):
        self.reader.create_report = MagicMock(return_value='report')
        for report in self.reader.run():
            self.assertEqual(report, 'report')
