
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
            'frequency': 'hours',
            'granularity': 'days'
        }
        self.config = {
            'query_folder': 'test/fixtures/queries',
            'output_folder': 'test/fixtures/output',
            'reportupdater-reports': {
                self.report_key: self.report_config
            },
            'defaults': {
                'db': 'db_key'
            }
        }
        self.reader = Reader(self.config)


    def test_get_type_when_type_not_in_config(self):
        report_config = {}
        result = self.reader.get_type(report_config)
        self.assertEqual(result, 'sql')


    def test_get_type_when_type_is_not_valid(self):
        report_config = {'type': 'not valid'}
        with self.assertRaises(ValueError):
            self.reader.get_type(report_config)


    def test_get_type_when_type_is_in_config(self):
        result = self.reader.get_type({'type': 'sql'})
        self.assertEqual(result, 'sql')
        result = self.reader.get_type({'type': 'script'})
        self.assertEqual(result, 'script')


    def test_get_frequency_and_granularity_when_value_is_not_in_config(self):
        report_config = {}
        with self.assertRaises(KeyError):
            self.reader.get_frequency(report_config)
        with self.assertRaises(KeyError):
            self.reader.get_granularity(report_config)


    def test_get_frequency_and_granularity_when_value_is_not_valid(self):
        report_config = {
            'frequency': 'wrong',
            'granularity': 'wrong'
        }
        with self.assertRaises(ValueError):
            self.reader.get_frequency(report_config)
        with self.assertRaises(ValueError):
            self.reader.get_granularity(report_config)


    def test_get_frequency_and_granularity(self):
        for frequency in ['hours', 'days', 'weeks', 'months']:
            report_config = {'frequency': frequency}
            result = self.reader.get_frequency(report_config)
            self.assertEqual(result, frequency)
        for granularity in ['days', 'weeks', 'months']:
            report_config = {'granularity': granularity}
            result = self.reader.get_granularity(report_config)
            self.assertEqual(result, granularity)


    def test_get_lag_when_value_is_not_in_config(self):
        report_config = {}
        result = self.reader.get_lag(report_config)
        self.assertEqual(result, 0)


    def test_get_lag_when_value_is_not_valid(self):
        report_config = {'lag': 'not an int'}
        with self.assertRaises(ValueError):
            self.reader.get_lag(report_config)
        report_config = {'lag': -1}
        with self.assertRaises(ValueError):
            self.reader.get_lag(report_config)


    def test_get_lag(self):
        report_config = {'lag': 10}
        result = self.reader.get_lag(report_config)
        self.assertEqual(result, 10)


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


    def test_get_db_key_when_in_report_config(self):
        db_key = 'some-db-key'
        report_config = {'db': db_key}
        reader = Reader({})
        result = reader.get_db_key(report_config)
        self.assertEqual(result, db_key)


    def test_get_db_key_when_report_config_db_is_not_a_string(self):
        db_key = ('not', 'a', 'string')
        report_config = {'db': db_key}
        reader = Reader({})
        with self.assertRaises(ValueError):
            reader.get_db_key(report_config)


    def test_get_db_key_when_defaults_is_not_in_config(self):
        report_config = {}
        reader = Reader({})
        with self.assertRaises(KeyError):
            reader.get_db_key(report_config)


    def test_get_db_key_when_defaults_db_is_not_in_config(self):
        config = {'defaults': {}}
        report_config = {}
        reader = Reader(config)
        with self.assertRaises(KeyError):
            reader.get_db_key(report_config)


    def test_get_db_key_when_defaults_db_is_not_a_string(self):
        config = {
            'defaults': {
                'db': None
            }
        }
        report_config = {}
        reader = Reader(config)
        with self.assertRaises(ValueError):
            reader.get_db_key(report_config)


    def test_get_db_key_when_in_defaults(self):
        report_config = {}
        result = self.reader.get_db_key(report_config)
        expected = self.config['defaults']['db']
        self.assertEqual(result, expected)


    def test_create_report_when_query_folder_is_not_in_config(self):
        reader = Reader({})
        with self.assertRaises(KeyError):
            reader.create_report('reader_test', {})


    def test_create_report_when_query_folder_is_not_a_string(self):
        reader = Reader({'query_folder': ('not', 'a', 'string')})
        with self.assertRaises(ValueError):
            reader.create_report('reader_test', {})


    def test_get_sql_template_when_query_folder_does_not_exist(self):
        with self.assertRaises(IOError):
            self.reader.get_sql_template('reader_test', 'nonexistent')


    def test_get_sql_template_when_sql_file_does_not_exist(self):
        query_folder = self.config['query_folder']
        with self.assertRaises(IOError):
            self.reader.get_sql_template('wrong_report_key', query_folder)


    def test_get_sql_template(self):
        report_key = 'reader_test'
        query_folder = self.config['query_folder']
        result = self.reader.get_sql_template(report_key, query_folder)
        sql_template_path = os.path.join(query_folder, report_key + '.sql')
        with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
            expected = sql_template_file.read()
        self.assertEqual(result, expected)


    def test_get_explode_by_wiki(self):
        self.config['wikis_path'] = 'test/fixtures/wikis.txt'
        result = self.reader.get_explode_by({})
        self.assertNotIn('wiki', result)
        result = self.reader.get_explode_by({'by_wiki': ('not', 'a', 'bool')})
        self.assertNotIn('wiki', result)
        result = self.reader.get_explode_by({'by_wiki': False})
        self.assertNotIn('wiki', result)
        result = self.reader.get_explode_by({'by_wiki': True})
        self.assertIn('wiki', result)
        self.assertEqual(result['wiki'], ['wiki1', 'wiki2', 'wiki3', 'all'])


    def test_get_explode_by(self):
        report_config = {
            'explode_by': {
                'editor': 'visualeditor, wikitext',
                'language': 'en, de, fr'
            }
        }
        result = self.reader.get_explode_by(report_config)
        expected = {
            'editor': ['visualeditor', 'wikitext'],
            'language': ['en', 'de', 'fr']
        }
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


    def test_create_sql_report(self):
        self.reader.get_type = MagicMock(return_value='sql')
        self.reader.get_first_date = MagicMock(return_value='first_date')
        self.reader.get_frequency = MagicMock(return_value='frequency')
        self.reader.get_granularity = MagicMock(return_value='granularity')
        self.reader.get_is_timeboxed = MagicMock(return_value='is_timeboxed')
        self.reader.get_is_funnel = MagicMock(return_value='is_funnel')
        self.reader.get_db_key = MagicMock(return_value='db_key')
        self.reader.get_sql_template = MagicMock(return_value='sql_template')
        self.reader.get_by_wiki = MagicMock(return_value=False)
        report = self.reader.create_report(self.report_key, self.report_config)
        self.assertEqual(report.key, self.report_key)
        self.assertEqual(report.type, 'sql')
        self.assertEqual(report.first_date, 'first_date')
        self.assertEqual(report.frequency, 'frequency')
        self.assertEqual(report.granularity, 'granularity')
        self.assertEqual(report.is_timeboxed, 'is_timeboxed')
        self.assertEqual(report.is_funnel, 'is_funnel')
        self.assertEqual(report.db_key, 'db_key')
        self.assertEqual(report.sql_template, 'sql_template')
        self.assertEqual(report.script, None)
        self.assertEqual(report.explode_by, {})
        self.assertEqual(report.results, {'header': [], 'data': {}})
        self.assertEqual(report.start, None)
        self.assertEqual(report.end, None)


    def test_create_script_report(self):
        self.reader.get_type = MagicMock(return_value='script')
        self.reader.get_first_date = MagicMock(return_value='first_date')
        self.reader.get_frequency = MagicMock(return_value='frequency')
        self.reader.get_granularity = MagicMock(return_value='granularity')
        self.reader.get_is_timeboxed = MagicMock(return_value='is_timeboxed')
        self.reader.get_is_funnel = MagicMock(return_value='is_funnel')
        self.reader.get_db_key = MagicMock(return_value='db_key')
        self.reader.get_sql_template = MagicMock(return_value='sql_template')
        self.reader.get_by_wiki = MagicMock(return_value=False)
        report = self.reader.create_report(self.report_key, self.report_config)
        self.assertEqual(report.key, self.report_key)
        self.assertEqual(report.type, 'script')
        self.assertEqual(report.first_date, 'first_date')
        self.assertEqual(report.frequency, 'frequency')
        self.assertEqual(report.granularity, 'granularity')
        self.assertEqual(report.is_timeboxed, 'is_timeboxed')
        self.assertEqual(report.is_funnel, 'is_funnel')
        self.assertEqual(report.db_key, None)
        self.assertEqual(report.sql_template, None)
        self.assertEqual(report.script, 'test/fixtures/queries/reader_test')
        self.assertEqual(report.explode_by, {})
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
