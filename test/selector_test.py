
import os
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from unittest import TestCase
from mock import MagicMock
from datetime import datetime
from dateutil.relativedelta import relativedelta


class SelectorTest(TestCase):


    def setUp(self):
        self.config = {
            'output_folder': 'test/fixtures/output',
            'last_exec_time': datetime(2015, 1, 2, 23, 50, 30),
            'current_exec_time': datetime(2015, 1, 3, 1, 20, 30)
        }
        reader = Reader(self.config)
        self.selector = Selector(reader, self.config)

        self.report = Report()
        self.report.key = 'selector_test'
        self.report.first_date = datetime(2015, 1, 1)
        self.report.frequency = 'hours'
        self.report.granularity = 'days'
        self.report.is_timeboxed = True


    def test_is_time_to_execute_when_last_exec_time_is_none(self):
        last_exec_time = None
        now = datetime.now()
        frequency = 'hours'
        is_time = self.selector.is_time_to_execute(last_exec_time, now, frequency)
        self.assertTrue(is_time)


    def test_is_time_to_execute_when_both_dates_are_in_the_same_hour(self):
        last_exec_time = datetime(2015, 1, 1, 3, 30, 0)
        now = datetime(2015, 1, 1, 3, 40, 0)
        frequency = 'hours'
        is_time = self.selector.is_time_to_execute(last_exec_time, now, frequency)
        self.assertFalse(is_time)


    def test_is_time_to_execute_when_both_dates_are_in_different_hours(self):
        last_exec_time = datetime(2015, 1, 1, 3, 30, 0)
        now = datetime(2015, 1, 1, 4, 20, 0)
        frequency = 'hours'
        is_time = self.selector.is_time_to_execute(last_exec_time, now, frequency)
        self.assertTrue(is_time)


    def test_is_time_to_execute_when_both_dates_are_in_the_same_day(self):
        last_exec_time = datetime(2015, 1, 1, 3, 30, 0)
        now = datetime(2015, 1, 1, 10, 40, 0)
        frequency = 'days'
        is_time = self.selector.is_time_to_execute(last_exec_time, now, frequency)
        self.assertFalse(is_time)


    def test_is_time_to_execute_when_both_dates_are_in_different_days(self):
        last_exec_time = datetime(2015, 1, 1, 3, 30, 0)
        now = datetime(2015, 1, 2, 4, 20, 0)
        frequency = 'days'
        is_time = self.selector.is_time_to_execute(last_exec_time, now, frequency)
        self.assertTrue(is_time)


    def test_get_interval_reports_when_previous_results_is_empty(self):
        # Note no previous results csv exists for default report.
        now = datetime(2015, 1, 2)
        reports = list(self.selector.get_interval_reports(self.report, now))
        self.assertEqual(len(reports), 2)
        self.assertEqual(reports[0].start, datetime(2015, 1, 1))
        self.assertEqual(reports[0].end, datetime(2015, 1, 2))
        self.assertEqual(reports[1].start, datetime(2015, 1, 2))
        self.assertEqual(reports[1].end, datetime(2015, 1, 3))


    def test_get_interval_reports_when_previous_results_has_some_dates(self):
        self.report.key = 'selector_test1'
        # see: test/fixtures/output/selector_test1.csv
        now = datetime(2015, 1, 2)
        reports = list(self.selector.get_interval_reports(self.report, now))
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0].start, datetime(2015, 1, 2))
        self.assertEqual(reports[0].end, datetime(2015, 1, 3))


    def test_get_interval_reports_when_previous_results_has_all_dates(self):
        self.report.key = 'selector_test2'
        # see: test/fixtures/output/selector_test2.csv
        now = datetime(2015, 1, 2)
        reports = list(self.selector.get_interval_reports(self.report, now))
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0].start, datetime(2015, 1, 2))
        self.assertEqual(reports[0].end, datetime(2015, 1, 3))


    def test_truncate_date_when_period_is_hours(self):
        date = datetime(2015, 1, 5, 10, 20, 30)
        result = self.selector.truncate_date(date, 'hours')
        expected = datetime(2015, 1, 5, 10, 0, 0)
        self.assertEqual(result, expected)


    def test_truncate_date_when_period_is_days(self):
        date = datetime(2015, 1, 5, 10, 20, 30)
        result = self.selector.truncate_date(date, 'days')
        expected = datetime(2015, 1, 5, 0, 0, 0)
        self.assertEqual(result, expected)


    def test_truncate_date_when_period_is_months(self):
        date = datetime(2015, 1, 5, 10, 20, 30)
        result = self.selector.truncate_date(date, 'months')
        expected = datetime(2015, 1, 1, 0, 0, 0)
        self.assertEqual(result, expected)


    def test_truncate_date_when_period_is_not_valid(self):
        date = datetime(2015, 1, 5, 10, 20, 30)
        with self.assertRaises(ValueError):
            self.selector.truncate_date(date, 'notvalid')


    def test_get_increment_when_period_is_days(self):
        increment = self.selector.get_increment('days')
        self.assertEqual(increment, relativedelta(days=1))


    def test_get_increment_when_period_is_months(self):
        increment = self.selector.get_increment('months')
        self.assertEqual(increment, relativedelta(months=1))


    def test_get_increment_when_period_is_invalid(self):
        with self.assertRaises(ValueError):
            self.selector.get_increment('notvalid')


    def test_get_all_start_dates_when_first_date_is_greater_than_current_date(self):
        first_date = datetime(2015, 1, 2)
        current_date = datetime(2015, 1, 1)
        increment = relativedelta(days=1)
        with self.assertRaises(ValueError):
            list(self.selector.get_all_start_dates(first_date, current_date, increment))


    def test_get_all_start_dates_when_increment_is_negative(self):
        first_date = datetime(2015, 1, 1)
        current_date = datetime(2015, 1, 2)
        increment = relativedelta(days=-1)
        with self.assertRaises(ValueError):
            list(self.selector.get_all_start_dates(first_date, current_date, increment))


    def test_get_all_start_dates_when_first_date_equals_current_date(self):
        date = datetime(2015, 1, 1)
        increment = relativedelta(days=1)
        all_dates = list(self.selector.get_all_start_dates(date, date, increment))
        self.assertEqual(len(all_dates), 1)
        self.assertEqual(all_dates[0], date)


    def test_get_all_start_dates_when_increment_is_days(self):
        first_date = datetime(2015, 1, 1)
        current_date = datetime(2015, 1, 3)
        increment = relativedelta(days=1)
        result = list(self.selector.get_all_start_dates(first_date, current_date, increment))
        expected = [
            datetime(2015, 1, 1),
            datetime(2015, 1, 2),
            datetime(2015, 1, 3)
        ]
        self.assertEqual(result, expected)


    def test_get_all_start_dates_when_increment_is_months(self):
        first_date = datetime(2015, 1, 1)
        current_date = datetime(2015, 3, 1)
        increment = relativedelta(months=1)
        result = list(self.selector.get_all_start_dates(first_date, current_date, increment))
        expected = [
            datetime(2015, 1, 1),
            datetime(2015, 2, 1),
            datetime(2015, 3, 1)
        ]
        self.assertEqual(result, expected)


    def test_run_when_last_exec_time_is_greater_than_current_exec_time(self):
        self.config['last_exec_time'] = datetime(2015, 1, 2)
        self.config['current_exec_time'] = datetime(2015, 1, 1)
        with self.assertRaises(ValueError):
            list(self.selector.run())


    def test_run_when_helper_method_raises_error(self):
        read = [self.report]
        self.selector.reader.run = MagicMock(return_value=read)
        self.selector.is_time_to_execute = MagicMock(side_effect=Exception())
        selected = list(self.selector.run())
        self.assertEqual(len(selected), 0)


    def test_run_when_not_is_time_to_execute(self):
        read = [self.report]
        self.selector.reader.run = MagicMock(return_value=read)
        self.selector.is_time_to_execute = MagicMock(return_value=False)
        selected = list(self.selector.run())
        self.assertEqual(len(selected), 0)


    def test_run_when_not_is_timeboxed(self):
        self.report.is_timeboxed = False
        read = [self.report]
        self.selector.reader.run = MagicMock(return_value=read)
        self.selector.is_time_to_execute = MagicMock(return_value=True)
        selected = list(self.selector.run())
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0], self.report)


    def test_run_when_is_timeboxed(self):
        self.report.is_timeboxed = True
        read = [self.report]
        self.selector.reader.run = MagicMock(return_value=read)
        self.selector.is_time_to_execute = MagicMock(return_value=True)
        self.selector.get_interval_reports = MagicMock(return_value=['report'])
        selected = list(self.selector.run())
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0], 'report')
