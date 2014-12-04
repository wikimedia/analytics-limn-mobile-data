# -*- coding: utf-8 -*-
import copy
import logging
import logging.config

__all__ = ['get_logger']


DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'stream': {
            'format': '[%(asctime)s %(name)s %(levelname)s] %(message)s'
        },
        'file': {
            'format': '[%(asctime)s %(levelname)s] %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'stream'
        },
        'log_to_file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'limn.log',
            'mode': 'a+',
            'formatter': 'file',
        },
        'logstash': {
            'level': 'INFO',
            'class': 'logstash.LogstashHandler',
            'host': 'localhost',
            'port': 5959,
            'version': 1,
            'message_type': 'logstash',
            'fqdn': False,
            'tags': ['limn_mobile_data'],
        }
    },
    'loggers': {
        'limn_mobile': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False
        }
    }
}


def parse_logstash_endpoint(logstash_endpoint):
    try:
        host, port = logstash_endpoint.split(':') if isinstance(
            logstash_endpoint, basestring) else (None, None)
        port = int(port) if port else None
    except ValueError:
        host, port = None, None
    return host, port


def update_config(source_config, config):
    """
    Update source config dictionary with values from
    another config dictionary in a granular way, rather
    than only top level substitution.
    """
    for key, value in config.items():
        source_value = source_config.get(key)
        if not source_value:
            continue
        if isinstance(source_value, dict) and isinstance(value, dict):
            update_config(source_value, value)
        elif isinstance(source_value, list) and not isinstance(value, list):
            source_value.append(value)
        elif type(source_value) == type(value):
            source_config[key] = value


def update_logging_config(default_config, config, log_file_path,
                          logstash_endpoint):
    """
    Update logging config dict.

    Args:
        default_config: A dictionary for default logging config.
        config: A dictionary for customizing logging config.
        log_file_path: A string
        logstash_endpoint: A string of the form 'host:port'

    Returns:
        A dictionary for the updated logging config.
    """
    updated_config = copy.deepcopy(default_config)
    if isinstance(config, dict):
        update_config(updated_config, config)
    config_handlers = updated_config['loggers']['limn_mobile']['handlers']
    if log_file_path:
        updated_config['handlers']['log_to_file']['filename'] = log_file_path
        if 'log_to_file' not in config_handlers:
            config_handlers.append('log_to_file')
    elif 'log_to_file' in config_handlers:
        config_handlers.remove('log_to_file')

    logstash_host, logstash_port = parse_logstash_endpoint(logstash_endpoint)
    if logstash_host and logstash_port:
        updated_config['handlers']['logstash'].update({
            'host': logstash_host,
            'port': logstash_port
        })
        if 'logstash' not in config_handlers:
            config_handlers.append('logstash')
    elif 'logstash' in config_handlers:
        config_handlers.remove('logstash')
    return updated_config


def get_logger(config={}, log_file_path=None, logstash_endpoint=None):
    """
    Get a logger instance for logging.
    """
    config = copy.deepcopy(config)
    logging_config = update_logging_config(
        DEFAULT_LOGGING_CONFIG, config, log_file_path, logstash_endpoint)
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger('limn_mobile')
    return logger
