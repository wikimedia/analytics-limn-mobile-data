
# This is the main module of the project.
#
# Its 'run' method will execute the whole pipeline:
#   1. Read the report information from config file
#   2. Select or triage the reports that have to be executed
#   3. Execute those reports against the database
#   4. Write / update the files with the results
#
# In addition to that, this module uses a pid file
# to avoid concurrent execution; blocking instances to run
# when another instance is already running.
#
# Also, it stores and controls the last execution time,
# used for report scheduling in the select step.


import os
import io
import errno
import yaml
import logging
from datetime import datetime
from reader import Reader
from selector import Selector
from executor import Executor
from writer import Writer
from utils import DATE_AND_TIME_FORMAT


def run(**kwargs):
    params = get_params(kwargs)
    configure_logging(params)

    if only_instance_running(params):
        logging.info('Starting execution.')
        write_pid_file(params)  # create lock to avoid concurrent executions

        current_exec_time = utcnow()
        last_exec_time = replace_exec_time(current_exec_time, params['history_path'])

        if 'config' in params:
            config = params['config']
        else:
            config = load_config(params['config_path'])
        config['current_exec_time'] = current_exec_time
        config['last_exec_time'] = last_exec_time
        config['sql_folder'] = params['sql_folder']
        config['output_folder'] = params['output_folder']
        config['wikis_path'] = params['wikis_path']

        reader = Reader(config)
        selector = Selector(reader, config)
        executor = Executor(selector, config)
        writer = Writer(executor, config)
        writer.run()

        delete_pid_file(params)  # free lock for other instances to execute
        logging.info('Execution complete.')
    else:
        logging.warning('Another instance is already running. Exiting.')


def get_params(passed_params):
    project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
    sql_folder = passed_params.pop('sql_folder', os.path.join(project_root, 'sql'))
    process_params = {
        'pid_file_path': os.path.join(sql_folder, '.reportupdater.pid'),
        'history_path': os.path.join(sql_folder, '.reportupdater.history'),
        'config_path': os.path.join(sql_folder, 'config.yaml'),
        'output_folder': os.path.join(project_root, 'output'),
        'wikis_path': os.path.join(project_root, 'reportupdater/wikis.txt'),
        'log_level': logging.WARNING
    }
    passed_params = {k: v for k, v in passed_params.iteritems() if v is not None}
    process_params.update(passed_params)
    process_params['sql_folder'] = sql_folder
    return process_params


def configure_logging(params):
    logger = logging.getLogger()
    if 'log_file' in params:
        handler = logging.FileHandler(params['log_file'])
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(params['log_level'])


def only_instance_running(params):
    if os.path.isfile(params['pid_file_path']):
        try:
            with io.open(params['pid_file_path'], 'r') as pid_file:
                pid = int(pid_file.read().strip())
        except IOError:
            # Permission error.
            # Another instance run by another user is still executing.
            logging.warning('An instance run by another user was found.')
            return False
        if pid_exists(pid):
            # Another instance is still executing.
            return False
        else:
            # Another instance terminated unexpectedly,
            # leaving the stale pid file there.
            return True
    else:
        return True


def pid_exists(pid):
    try:
        # Sending signal 0 to a pid will raise an OSError exception
        # if the pid is not running, and do nothing otherwise.
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # No such process.
            return False
        elif err.errno == errno.EPERM:
            # Valid process, no permits.
            return True
        else:
            raise
    else:
        return True


def write_pid_file(params):
    logging.info('Writing the pid file.')
    pid = os.getpid()
    with io.open(params['pid_file_path'], 'w') as pid_file:
        pid_file.write(unicode(pid))


def delete_pid_file(params):
    logging.info('Deleting the pid file.')
    try:
        os.remove(params['pid_file_path'])
    except OSError, e:
        logging.error('Unable to delete the pid file (' + str(e) + ').')


def replace_exec_time(current_time, history_path):
    # Writes the current execution time to the history file.
    # If the file contains the last execution time, it is returned.
    if os.path.exists(history_path):
        with io.open(history_path) as history_file:
            last_time_str = history_file.read().strip()
            last_time = datetime.strptime(last_time_str, DATE_AND_TIME_FORMAT)
    else:
        last_time = None
    with io.open(history_path, 'w') as history_file:
        current_time_str = current_time.strftime(DATE_AND_TIME_FORMAT)
        history_file.write(unicode(current_time_str))
    return last_time


def load_config(config_path):
    try:
        with io.open(config_path, encoding='utf-8') as config_file:
            return yaml.load(config_file)
    except IOError, e:
        raise IOError('Can not read the config file because of: (' + str(e) + ').')


def utcnow():
    return datetime.utcnow()
