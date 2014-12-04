# -*- coding: utf-8 -*-
import unittest
import logging
import copy
from logger import (
    update_config, update_logging_config, get_logger,
    parse_logstash_endpoint)


class TestUpdateConfig(unittest.TestCase):

    def test_success(self):
        SOURCE_CONFIG = {
            'a': 1,
            'b': {
                'b1': ['blah']
            },
            'c': ['foo']
        }

        CONFIG = {
            'a': 2,
            'b': {
                'b1': 'foobar',
                'b2': 'whatever'
            },
            'c': [1, 2]
        }
        UPDATED_CONFIG = {
            'a': 2,
            'b': {
                'b1': ['blah', 'foobar']
            },
            'c': [1, 2]
        }
        update_config(SOURCE_CONFIG, CONFIG)
        self.assertEqual(SOURCE_CONFIG, UPDATED_CONFIG)


class TestUpdateLoggingConfig(unittest.TestCase):

    def test_success(self):
        from logger import DEFAULT_LOGGING_CONFIG
        UPDATED_LOGGING = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        UPDATED_LOGGING['loggers']['limn_mobile']['handlers'] = [
            'console', 'logstash', 'log_to_file']
        UPDATED_LOGGING['handlers']['log_to_file']['filename'] = 'test.log'
        UPDATED_LOGGING['handlers']['logstash'].update({
            'host': 'foo',
            'port': 8000
        })
        updated_logging_config = update_logging_config(
            DEFAULT_LOGGING_CONFIG, {
                'loggers': {
                    'limn_mobile': {
                        'handlers': ['console', 'logstash']
                    }
                }
            }, 'test.log', 'foo:8000')
        self.assertEqual(updated_logging_config, UPDATED_LOGGING)


class TestParseLogstashEndpoint(unittest.TestCase):

    def test_success(self):
        host, port = parse_logstash_endpoint('foo:8000')
        self.assertEqual(host, 'foo')
        self.assertEqual(port, 8000)

    def test_error(self):
        host, port = parse_logstash_endpoint('foo:bar')
        self.assertIsNone(host)
        self.assertIsNone(port)
        host, port = parse_logstash_endpoint('foo')
        self.assertIsNone(host)
        self.assertIsNone(port)
        host, port = parse_logstash_endpoint(None)
        self.assertIsNone(host)
        self.assertIsNone(port)


class TestGetLogger(unittest.TestCase):

    def test_success(self):
        logger = get_logger(
            config={
                'loggers': {
                    'limn_mobile': {
                        'handlers': ['console', 'logstash']
                    }
                }
            },
            log_file_path='test.log')
        self.assertTrue(isinstance(logger, logging.Logger))
        self.assertEqual(len(logger.handlers), 2)
