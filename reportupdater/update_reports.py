#!/usr/bin/python

import logging
import argparse
import reportupdater


LOGGING_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def main():
    parser = argparse.ArgumentParser(description='Write/Update SQL reports into CSV files.')
    parser.add_argument('config_path', help='Yaml configuration file path.')
    parser.add_argument('sql_folder', help='Folder with *.sql files.')
    parser.add_argument('output_folder', help='Folder to write the *.csv files to.')
    parser.add_argument('-l', '--log-level', help='(debug|info|warning|error|critical)')
    args = vars(parser.parse_args())
    if 'log_level' in args:
        if args['log_level'] in LOGGING_LEVELS:
            args['log_level'] = LOGGING_LEVELS[args['log_level']]
        else:
            del args['log_level']
    reportupdater.run(**args)


if __name__ == '__main__':
    main()
