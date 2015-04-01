
from reportupdater.executor import Executor
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from reportupdater.utils import TIMESTAMP_FORMAT
from test_utils import ConnectionMock
from unittest import TestCase
from mock import MagicMock
from datetime import datetime, date
import MySQLdb


class ExecutorTest(TestCase):


    def setUp(self):
        self.db_key = 'executor_test'
        self.db_config = {
            'host': 'some.host',
            'port': 12345,
            'creds_file': '/some/creds/file',
            'db': 'database'
        }
        self.config = {
            'databases': {
                self.db_key: self.db_config
            }
        }
        reader = Reader(self.config)
        selector = Selector(reader, self.config)
        self.executor = Executor(selector, self.config)

        self.report = Report()
        self.report.is_timeboxed = True
        self.report.start = datetime(2015, 1, 1)
        self.report.end = datetime(2015, 1, 2)
        self.report.db_key = self.db_key
        self.report.sql_template = ('SELECT date, value FROM table '
                                    'WHERE date >= {from_timestamp} '
                                    'AND date < {to_timestamp};')


    def test_instantiate_sql_when_report_is_timeboxed_and_format_raises_error(self):
        self.report.sql_template = 'SOME sql WITH AN {unknown} placeholder;'
        with self.assertRaises(ValueError):
            self.executor.instantiate_sql(self.report)


    def test_instantiate_sql_when_report_is_timeboxed(self):
        result = self.executor.instantiate_sql(self.report)
        expected = self.report.sql_template.format(
            from_timestamp=self.report.start.strftime(TIMESTAMP_FORMAT),
            to_timestamp=self.report.end.strftime(TIMESTAMP_FORMAT)
        )
        self.assertEqual(result, expected)


    def test_instantiate_sql_when_report_is_not_timeboxed(self):
        self.report.is_timeboxed = False
        self.report.sql_template = 'SOME sql CODE;'
        sql_query = self.executor.instantiate_sql(self.report)
        self.assertEqual(sql_query, self.report.sql_template)


    def test_instantiate_sql_when_by_wiki_is_true(self):
        self.report.is_timeboxed = False
        self.report.explode_by = {'wiki': 'wiki'}
        self.report.sql_template = 'SOME sql WITH "{wiki}";'
        sql_query = self.executor.instantiate_sql(self.report)
        self.assertEqual(sql_query, 'SOME sql WITH "wiki";')


    def test_create_connection_when_db_key_is_not_in_db_config(self):
        del self.config['databases'][self.db_key]
        with self.assertRaises(KeyError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_db_config_is_not_a_dict(self):
        self.config['databases'][self.db_key] = 'not a dict'
        with self.assertRaises(ValueError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_host_is_not_in_config(self):
        del self.db_config['host']
        with self.assertRaises(KeyError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_port_is_not_in_config(self):
        del self.db_config['port']
        with self.assertRaises(KeyError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_creds_file_is_not_in_config(self):
        del self.db_config['creds_file']
        with self.assertRaises(KeyError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_db_is_not_in_config(self):
        del self.db_config['db']
        with self.assertRaises(KeyError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_host_is_not_a_string(self):
        self.db_config['host'] = ('not', 'a', 'string')
        with self.assertRaises(ValueError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_port_is_not_an_integer(self):
        self.db_config['port'] = ('not', 'an', 'integer')
        with self.assertRaises(ValueError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_creds_file_is_not_a_string(self):
        self.db_config['creds_file'] = ('not', 'a', 'string')
        with self.assertRaises(ValueError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_db_is_not_a_string(self):
        self.db_config['db'] = ('not', 'a', 'string')
        with self.assertRaises(ValueError):
            self.executor.create_connection(self.db_key)


    def test_create_connection_when_mysqldb_connect_raises_error(self):
        mysqldb_connect_stash = MySQLdb.connect
        MySQLdb.connect = MagicMock(side_effect=Exception())
        with self.assertRaises(RuntimeError):
            self.executor.create_connection(self.db_key)
        MySQLdb.connect = mysqldb_connect_stash


    def test_create_connection(self):
        mysqldb_connect_stash = MySQLdb.connect
        MySQLdb.connect = MagicMock(return_value='connection')
        connection = self.executor.create_connection(self.db_key)
        self.assertEqual(connection, 'connection')
        MySQLdb.connect = mysqldb_connect_stash


    def test_execute_sql_when_mysqldb_execution_raises_error(self):
        def execute_callback(sql_query):
            raise Exception()
        connection = ConnectionMock(execute_callback, None, [])
        with self.assertRaises(RuntimeError):
            self.executor.execute_sql('SOME sql;', connection)


    def test_execute_sql_when_first_column_is_not_a_date(self):
        def fetchall_callback():
            return [
                [date(2015, 1, 1), '1'],
                ['bad formated date', '2']
            ]
        connection = ConnectionMock(None, fetchall_callback, [])
        with self.assertRaises(ValueError):
            self.executor.execute_sql('SOME sql;', connection)


    def test_execute_sql_with_funnel_data(self):
        def fetchall_callback():
            return [
                [date(2015, 1, 1), '1'],
                [date(2015, 1, 1), '2'],
                [date(2015, 1, 1), '3'],
                [date(2015, 1, 2), '4'],
                [date(2015, 1, 2), '5']
            ]
        connection = ConnectionMock(None, fetchall_callback, [])
        result = self.executor.execute_sql('SOME sql;', connection, is_funnel=True)
        expected = {
            'header': [],
            'data': {
                datetime(2015, 1, 1): [
                    [datetime(2015, 1, 1), '1'],
                    [datetime(2015, 1, 1), '2'],
                    [datetime(2015, 1, 1), '3']
                ],
                datetime(2015, 1, 2): [
                    [datetime(2015, 1, 2), '4'],
                    [datetime(2015, 1, 2), '5']
                ]
            }
        }
        self.assertEqual(result, expected)


    def test_execute_sql(self):
        def fetchall_callback():
            return [
                [date(2015, 1, 1), '1'],
                [date(2015, 1, 2), '2']
            ]
        connection = ConnectionMock(None, fetchall_callback, [])
        result = self.executor.execute_sql('SOME sql;', connection)
        expected = {
            'header': [],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), '1'],
                datetime(2015, 1, 2): [datetime(2015, 1, 2), '2'],
            }
        }
        self.assertEqual(result, expected)


    def test_run_when_databases_is_not_in_config(self):
        del self.config['databases']
        with self.assertRaises(KeyError):
            list(self.executor.run())


    def test_run_when_config_databases_is_not_a_dict(self):
        self.config['databases'] = 'not a dict'
        with self.assertRaises(ValueError):
            list(self.executor.run())


    def test_run_when_helper_method_raises_error(self):
        selected = [self.report]
        self.executor.selector.run = MagicMock(return_value=selected)
        self.executor.instantiate_sql = MagicMock(side_effect=Exception())
        executed = list(self.executor.run())
        self.assertEqual(len(executed), 0)


    def test_run(self):
        selected = [self.report]
        self.executor.selector.run = MagicMock(return_value=selected)
        self.executor.create_connection = MagicMock(return_value='connection')
        results = {
            'header': ['some', 'sql', 'header'],
            'data': {datetime(2015, 1, 1): [date(2015, 1, 1), 'some', 'value']}
        }
        self.executor.execute_sql = MagicMock(return_value=results)
        executed = list(self.executor.run())
        self.assertEqual(len(executed), 1)
        report = executed[0]
        self.assertEqual(report.results, results)
