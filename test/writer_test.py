
import os
import io
from reportupdater.writer import Writer
from reportupdater.executor import Executor
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from unittest import TestCase
from datetime import datetime
from mock import MagicMock


class WriterTest(TestCase):


    def setUp(self):
        self.config = {
            'output_folder': 'test/fixtures/output'
        }
        reader = Reader(self.config)
        selector = Selector(reader, self.config)
        executor = Executor(selector, self.config)
        self.writer = Writer(executor, self.config)

        self.report = Report()
        self.report.key = 'writer_test'
        self.report.sql_template = 'SOME sql TEMPLATE;'
        self.report.results = {
            'header': ['date', 'value'],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), '1']
            }
        }

        self.io_open_stash = io.open
        self.os_rename_stash = os.rename
        self.paths_to_clean = []


    def tearDown(self):
        try:
            os.remove('test/fixtures/output/writer_test.csv')
        except:
            pass
        io.open = self.io_open_stash
        os.rename = self.os_rename_stash
        for path in self.paths_to_clean:
            try:
                os.remove(path)
            except:
                pass


    def test_write_results_when_io_open_raises_error(self):
        io.open = MagicMock(side_effect=Exception())
        header = self.report.results['header']
        data = self.report.results['data']
        output_folder = self.config['output_folder']
        with self.assertRaises(RuntimeError):
            self.writer.write_results(header, data, self.report, output_folder)


    def test_write_results_when_os_rename_raises_error(self):
        os.rename = MagicMock(side_effect=Exception())
        header = self.report.results['header']
        data = self.report.results['data']
        output_folder = self.config['output_folder']
        with self.assertRaises(RuntimeError):
            self.writer.write_results(header, data, self.report, output_folder)


    def test_write_results_when_results_data_is_empty(self):
        header = ['date', 'value']
        data = {}
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.csv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output = output_file.read().strip()
        self.assertEqual(output, 'date,value')


    def test_write_results_with_funnel_data(self):
        self.report.is_funnel = True
        header = ['date', 'value']
        data = {
            datetime(2015, 1, 2): [[datetime(2015, 1, 2), 'c'], [datetime(2015, 1, 2), 'd']],
            datetime(2015, 1, 3): [[datetime(2015, 1, 3), 'e']],
            datetime(2015, 1, 1): [[datetime(2015, 1, 1), 'a'], [datetime(2015, 1, 1), 'b']]
        }
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.csv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 6)
        self.assertEqual(output_lines[0], 'date,value\n')
        self.assertEqual(output_lines[1], '2015-01-01,a\n')
        self.assertEqual(output_lines[2], '2015-01-01,b\n')
        self.assertEqual(output_lines[3], '2015-01-02,c\n')
        self.assertEqual(output_lines[4], '2015-01-02,d\n')
        self.assertEqual(output_lines[5], '2015-01-03,e\n')


    def test_write_results(self):
        header = ['date', 'value']
        data = {
            datetime(2015, 1, 2): [datetime(2015, 1, 2), 'a'],
            datetime(2015, 1, 3): [datetime(2015, 1, 3), 'b'],
            datetime(2015, 1, 1): [datetime(2015, 1, 1), 'c']
        }
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.csv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 4)
        self.assertEqual(output_lines[0], 'date,value\n')
        self.assertEqual(output_lines[1], '2015-01-01,c\n')
        self.assertEqual(output_lines[2], '2015-01-02,a\n')
        self.assertEqual(output_lines[3], '2015-01-03,b\n')


    def test_run_when_previous_results_header_is_empty(self):
        # self.report has no previous results csv by default setup
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.write_results = MagicMock()
        self.writer.run()
        self.writer.write_results.assert_called_once_with(
            self.report.results['header'],
            self.report.results['data'],
            self.report,
            self.config['output_folder']
        )


    def test_run_when_previous_results_header_and_results_header_are_different(self):
        self.report.key = 'writer_test1'
        # previous header will be: ['date', 'val1', 'val2', 'val3']
        # see: test/fixtures/output/writer_test1.csv
        self.report.results = {
            'header': ['date', 'val4', 'val5'],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), 4, 5]
            }
        }
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        with self.assertRaises(ValueError):
            self.writer.run()


    def test_run_when_helper_method_raises_error(self):
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.write_results = MagicMock(side_effect=Exception())
        self.writer.run()
        # just checking no error is raised


    def test_run(self):
        self.report.key = 'writer_test2'
        output_path = os.path.join(self.config['output_folder'], self.report.key + '.csv')
        self.paths_to_clean.append(output_path)
        # Set up previous results.
        # File can not be a permanent fixture, because it is overwritten be the test.
        with io.open(output_path, 'w') as output_file:
            output_file.write(unicode('date,value\n2015-01-01,a\n2015-01-02,b\n'))
        # Set up current results.
        self.report.results['header'] = ['date', 'value']
        self.report.results['data'] = {
            datetime(2015, 1, 1): [datetime(2015, 1, 1), 'a'],
            datetime(2015, 1, 2): [datetime(2015, 1, 2), 'b'],
            datetime(2015, 1, 3): [datetime(2015, 1, 3), 'c']
        }
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.run()
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 4)
        self.assertEqual(output_lines[0], 'date,value\n')
        self.assertEqual(output_lines[1], '2015-01-01,a\n')
        self.assertEqual(output_lines[2], '2015-01-02,b\n')
        self.assertEqual(output_lines[3], '2015-01-03,c\n')
