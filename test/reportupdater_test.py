
import os
import io
import time
import MySQLdb
from reportupdater import reportupdater
from reportupdater.utils import DATE_AND_TIME_FORMAT, DATE_FORMAT
from test_utils import ConnectionMock
from unittest import TestCase
from mock import MagicMock
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from threading import Thread


class ReportUpdaterTest(TestCase):


    def setUp(self):
        self.mysqldb_connect_stash = MySQLdb.connect
        self.utcnow_stash = reportupdater.utcnow
        self.paths_to_clean = [reportupdater.PID_FILE_PATH]
        self.config_folder = 'test/fixtures/config'
        self.sql_folder = 'test/fixtures/sql'
        self.output_folder = 'test/fixtures/output'
        self.history_path = 'test/fixtures/reportupdater_test.history'


    def tearDown(self):
        MySQLdb.connect = self.mysqldb_connect_stash
        reportupdater.utcnow = self.utcnow_stash
        for path in self.paths_to_clean:
            try:
                os.remove(path)
            except:
                pass


    def test_when_current_exec_time_and_last_exec_time_are_within_the_same_hour(self):
        last_exec_time = datetime(2015, 1, 2, 3, 4, 5)
        self.write_time_to_history(last_exec_time)
        reportupdater.utcnow = MagicMock(return_value=datetime(2015, 1, 2, 3, 40, 50))
        reportupdater.run(
            config_path=os.path.join(self.config_folder, 'reportupdater_test1.yaml'),
            sql_folder=self.sql_folder,
            output_folder=self.output_folder,
            history_path=self.history_path
        )
        # The report should not be computed because it has already been computed
        # within this hour. So the output file should not exist.
        output_path = os.path.join(self.output_folder, 'reportupdater_test1.csv')
        self.assertFalse(os.path.exists(output_path))


    def test_when_current_exec_time_and_last_exec_time_are_within_the_same_day(self):
        last_exec_time = datetime(2015, 1, 2, 3, 4, 5)
        self.write_time_to_history(last_exec_time)
        reportupdater.utcnow = MagicMock(return_value=datetime(2015, 1, 2, 13, 14, 15))
        reportupdater.run(
            config_path=os.path.join(self.config_folder, 'reportupdater_test2.yaml'),
            sql_folder=self.sql_folder,
            output_folder=self.output_folder,
            history_path=self.history_path
        )
        # The report should not be computed because it has already been computed
        # within this day. So the output file should not exist.
        output_path = os.path.join(self.output_folder, 'reportupdater_test2.csv')
        self.assertFalse(os.path.exists(output_path))


    def test_when_two_threads_run_reportupdater_in_parallel(self):
        # Mock database methods.
        def fetchall_callback():
            return []
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)

        def connect_with_lag(**kwargs):
            # This makes the connection take some time to execute,
            # thus giving time to the second thread to start.
            time.sleep(0.3)
            return connection_mock
        MySQLdb.connect = MagicMock(wraps=connect_with_lag)

        # The first thread should execute normally and output the results.
        history_path1 = 'test/fixtures/reportupdater_test1.history'
        output_path1 = os.path.join(self.output_folder, 'reportupdater_test1.csv')
        self.paths_to_clean.extend([history_path1, output_path1])
        args1 = {
            'config_path': os.path.join(self.config_folder, 'reportupdater_test1.yaml'),
            'sql_folder': self.sql_folder,
            'output_folder': self.output_folder,
            'history_path': history_path1
        }
        thread1 = Thread(target=reportupdater.run, kwargs=args1)
        thread1.start()

        # The second thread will start when the first thread is still running,
        # so it should be discarded by the pidfile control
        # and no output should be written.
        # Note that the history file is different, so that
        # the frequency control does not discard this thread.
        time.sleep(0.1)
        history_path2 = 'test/fixtures/reportupdater_test2.history'
        output_path2 = os.path.join(self.output_folder, 'reportupdater_test2.csv')
        self.paths_to_clean.extend([history_path2, output_path2])
        args2 = {
            'config_path': os.path.join(self.config_folder, 'reportupdater_test2.yaml'),
            'sql_folder': self.sql_folder,
            'output_folder': self.output_folder,
            'history_path': history_path2
        }
        thread2 = Thread(target=reportupdater.run, kwargs=args2)
        thread2.start()

        # wait for the threads to finish and assert results
        thread1.join()
        output_path1 = os.path.join(self.output_folder, 'reportupdater_test1.csv')
        self.assertTrue(os.path.exists(output_path1))
        thread2.join()
        output_path2 = os.path.join(self.output_folder, 'reportupdater_test2.csv')
        self.assertFalse(os.path.exists(output_path2))


    def test_hourly_timeboxed_report_without_previous_results(self):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(days=+1)
                value = self.last_value + 1
            except AttributeError:
                sql_date = date(2015, 1, 1)
                value = 1
            self.last_date = sql_date
            self.last_value = value
            return [[sql_date, str(value)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        MySQLdb.connect = MagicMock(return_value=connection_mock)

        config_path = os.path.join(self.config_folder, 'reportupdater_test1.yaml')
        output_path = os.path.join(self.output_folder, 'reportupdater_test1.csv')
        history_path = 'test/fixtures/reportupdater_test1.history'
        self.paths_to_clean.extend([output_path, history_path])
        reportupdater.run(
            config_path=config_path,
            sql_folder=self.sql_folder,
            output_folder=self.output_folder,
            history_path=history_path
        )
        self.assertTrue(os.path.exists(output_path))
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertTrue(len(output_lines) > 1)
        header = output_lines.pop(0).strip()
        self.assertEqual(header, 'date,value')
        # Assert that all lines hold subsequent values.
        expected_date = datetime(2015, 1, 1)
        expected_value = 1
        for line in output_lines:
            expected_line = expected_date.strftime(DATE_FORMAT) + ',' + str(expected_value)
            self.assertEqual(line.strip(), expected_line)
            expected_date += relativedelta(days=+1)
            expected_value += 1


    def test_hourly_funnel_timeboxed_report_without_previous_results(self):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(days=+1)
            except AttributeError:
                sql_date = date(2015, 1, 1)
            self.last_date = sql_date
            return [
                [sql_date, '1'],
                [sql_date, '2'],
                [sql_date, '3']
            ]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        MySQLdb.connect = MagicMock(return_value=connection_mock)

        config_path = os.path.join(self.config_folder, 'reportupdater_test3.yaml')
        output_path = os.path.join(self.output_folder, 'reportupdater_test3.csv')
        history_path = 'test/fixtures/reportupdater_test3.history'
        self.paths_to_clean.extend([output_path, history_path])
        reportupdater.run(
            config_path=config_path,
            sql_folder=self.sql_folder,
            output_folder=self.output_folder,
            history_path=history_path
        )
        self.assertTrue(os.path.exists(output_path))
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertTrue(len(output_lines) > 1)
        header = output_lines.pop(0).strip()
        self.assertEqual(header, 'date,value')
        # Assert that all lines hold subsequent values.
        expected_date = datetime(2015, 1, 1)
        expected_value = 1
        for line in output_lines:
            expected_line = expected_date.strftime(DATE_FORMAT) + ',' + str(expected_value)
            self.assertEqual(line.strip(), expected_line)
            if expected_value < 3:
                expected_value += 1
            else:
                expected_date += relativedelta(days=+1)
                expected_value = 1


    def test_daily_timeboxed_report_with_previous_results(self):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                date = self.last_date + relativedelta(months=+1)
                value = self.last_value + 1
            except AttributeError:
                # Starts at Mar, Jan and Feb are in previous results
                date = datetime(2015, 3, 1)
                value = 3
            self.last_date = date
            self.last_value = value
            return [[date.strftime(DATE_FORMAT), str(value)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        MySQLdb.connect = MagicMock(return_value=connection_mock)

        config_path = os.path.join(self.config_folder, 'reportupdater_test2.yaml')
        output_path = os.path.join(self.output_folder, 'reportupdater_test2.csv')
        history_path = 'test/fixtures/reportupdater_test2.history'
        with io.open(output_path, 'w') as output_file:
            output_file.write(unicode('date,value\n2015-01-01,1\n2015-02-01,2\n'))
        self.paths_to_clean.extend([output_path, history_path])
        reportupdater.run(
            config_path=config_path,
            sql_folder=self.sql_folder,
            output_folder=self.output_folder,
            history_path=history_path
        )
        self.assertTrue(os.path.exists(output_path))
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertTrue(len(output_lines) > 1)
        header = output_lines.pop(0).strip()
        self.assertEqual(header, 'date,value')
        # Assert that all lines hold subsequent values.
        expected_date = datetime(2015, 1, 1)
        expected_value = 1
        for line in output_lines:
            expected_line = expected_date.strftime(DATE_FORMAT) + ',' + str(expected_value)
            self.assertEqual(line.strip(), expected_line)
            expected_date += relativedelta(months=+1)
            expected_value += 1


    def write_time_to_history(self, last_exec_time):
        last_exec_time_str = last_exec_time.strftime(DATE_AND_TIME_FORMAT)
        with io.open(self.history_path, 'w') as history_file:
            history_file.write(unicode(last_exec_time_str))
        self.paths_to_clean.append(self.history_path)
