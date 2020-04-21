import optparse
import json
import os
from datetime import datetime, timedelta

import psycopg2

from corona_data_collector.DBToFileWriter import DBToFileWriter
from corona_data_collector.config import (
    db_settings, query_batch_size, use_gps_finder, destination_archive,
    DictObject, process_max_rows
)
from corona_data_collector.questionare_versions import questionare_versions


def run_query(settings, min_date, max_date, num_of_records=100):
    connection = None
    cursor = None
    supported_versions = ''
    for version in questionare_versions.keys():
        supported_versions += f'\'{version}\','
    supported_versions = supported_versions[:-1]
    try:
        fetched_rows = []
        connection = psycopg2.connect(host=settings['host'], user=settings['username'], password=settings['password'],
                                      sslmode='verify-ca', sslrootcert=settings['sslrootcert'],
                                      sslcert=settings['sslcert'], sslkey=settings['sslkey'], database="reports")
        cursor = connection.cursor()
        collected_all_records = False
        collected_records_sum = 0
        while not collected_all_records and collected_records_sum <= process_max_rows:
            cursor.execute(
                f"SELECT * FROM reports "
                f"where created > '{min_date}' and created < '{max_date}'"
                f" and data->>'version' in ({supported_versions})"
                f" limit {num_of_records}"
            )
            records = cursor.fetchall()
            if len(records) == 0:
                collected_all_records = True
            else:
                collected_records_sum += len(records)
                for record in records:
                    if record[2] is not None and len(record[2]) > 0:
                        data_dict = record[2]
                        data_dict['id'] = record[0]
                        data_dict['created'] = record[1].isoformat()
                        fetched_rows.append(data_dict)
                min_date = records[-1][1]
        return fetched_rows
    except (Exception, psycopg2.Error) as err:
        print("Database error", err)
    finally:
        if connection:
            cursor.close()
            connection.close()

            print('Database connection closed')


def get_process_arguments(parameters=None):
    if parameters is None:
        parser = optparse.OptionParser()
        parser.add_option('-s', action='store', dest='source', default='db')
        parser.add_option('-f', action='store', dest='file_path', default='')
        opts, args = parser.parse_args()
    else:
        opts = DictObject(
            source=parameters.get('source', 'db'),
            file_path=parameters.get('file_path', '')
        )
        args = DictObject()
    if opts.source not in ('db', 'file'):
        raise Exception('source (-s) argument must be either "file" or "db"')
    if opts.source == 'file' and opts.file_path == '':
        raise Exception('If you declare a file as your source, you must provide a path to the file you want to read')
    return opts, args


def main(parameters=None):
    yesterday = datetime.now() - timedelta(days=1)
    day = yesterday.day
    month = yesterday.month
    from_date = datetime(2020, month, day, 00, 00, 00)
    to_date = datetime(2020, month, day, 23, 59, 59)
    db_to_file_writer = DBToFileWriter(os.path.join(os.path.dirname(__file__), f'corona_bot_answers_{day}_{month}_2020.csv'))
    options, arguments = get_process_arguments(parameters)
    if options.source == 'file':
        print(f'Loading data from {options.file_path}')
        with open(options.file_path, "r") as data_source_file:
            db_to_file_writer.resultSet = json.load(data_source_file)
        db_to_file_writer.log_database_data()
        source_filename = options.file_path.split('/')[-1]
        os.rename(options.file_path, f'{destination_archive}/{source_filename}')
    elif options.source == 'db':
        collected_rows = run_query(db_settings, from_date, to_date, query_batch_size)
        if collected_rows is not None and len(collected_rows) > 0:
            print(f'Number of DB rows collected: {len(collected_rows)}')
            db_to_file_writer.resultSet = collected_rows
            from_date = collected_rows[-1]['created']
            db_to_file_writer.log_database_data()
    print('Adding GPS coordinates to records selected')
    db_to_file_writer.add_gps_coordinates(use_gps_finder)
    db_to_file_writer.clear_output_files()
    print('Operation cycle completed successfully')


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print('failed to run data collector: ', err)
        continue_running = False
    exit()
