import logging
from corona_data_collector.main import main
import os
from avid_covider_pipelines.utils import keep_last_runs_history, hash_updated_files, get_hash, get_github_sha, dump_to_path
from glob import glob
import datetime
from dataflows import update_resource, Flow, load
import contextlib
import sys


def store_destination_output_package(output_dir):
    last_package = {}
    if os.path.exists(os.path.join(output_dir, "destination_output", "datapackage.json")):

        def _load_last_package(row):
            last_package[row['name']] = row
            yield row

        Flow(
            load(os.path.join(output_dir, "destination_output", "datapackage.json")),
            _load_last_package
        ).process()

    def _files_list():
        for filename in glob(os.path.join(output_dir, "destination_output", "*")):
            if os.path.isfile(filename):
                name = os.path.relpath(filename, os.path.join(output_dir, "destination_output"))
                size = os.path.getsize(filename)
                hash = get_hash(filename)
                last_row = last_package.get(name)
                if last_row and hash == last_row.get('hash') and size == last_row['size']:
                    mtime = last_row['mtime']
                else:
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(filename))
                yield {"name": name, "size": size, "mtime": mtime, "hash": hash}

    Flow(
        _files_list(),
        update_resource(-1, name='files_list', path='files_list.csv'),
        dump_to_path(os.path.join(output_dir, "destination_output")),
    ).process()


class StdoutWriter:

    def __init__(self, f, echo_log):
        self.f = f
        self.echo_log = echo_log

    def write(self, message):
        message = message.strip()
        if message:
            if self.echo_log:
                logging.info(message)
            self.f.write(message + "\n")

    def flush(self):
        self.f.flush()


class StderrWriter:

    def __init__(self, f):
        self.f = f

    def write(self, message):
        message = message.strip()
        if message:
            logging.error(message)
            self.f.write(message + "\n")

    def flush(self):
        self.f.flush()


def run_log_main(parameters, run_row, echo_log, main_parameters):
    output_dir = parameters.get('output-dir', 'data/corona_data_collector')
    destination_output = os.path.join(output_dir, 'destination_output')
    log_files_dir = os.path.join(output_dir, "log_files")
    os.makedirs(log_files_dir, exist_ok=True)
    with open(os.path.join(log_files_dir, '%s.log' % run_row['start_time'].strftime('%Y%m%dT%H%M%S')), "a") as f:
        with contextlib.redirect_stderr(StderrWriter(f)):
            with contextlib.redirect_stdout(StdoutWriter(f, echo_log)):
                return main({"destination_output": destination_output, **main_parameters})


def corona_data_collector_main(last_run_row, run_row, output_dir, parameters):
    updated_file_hashes = {}

    def _run_callback():
        os.makedirs('%s/destination_output' % output_dir, exist_ok=True)
        run_row['exception'] = ''
        logging.info('Running corona_data_collector latest')
        now = datetime.datetime.now()
        try:
            run_log_main(parameters, run_row, True, {'source': 'db', 'day': now.day, 'month': now.month})
        except Exception as e:
            logging.exception(str(e))
            run_row['exception'] = 'exception for latest: ' + str(e)
            return
        logging.info('Running corona_data_collector for yesterday')
        try:
            for k, v in run_log_main(parameters, run_row, True, {'source': 'db'}).items():
                run_row[k] = v
        except Exception as e:
            logging.exception(str(e))
            run_row['exception'] = 'exception for yesterday: ' + str(e)
            return
        start_date = datetime.datetime(2020, 3, 22)
        end_date = datetime.datetime.now() - datetime.timedelta(days=2)
        days_months = []
        while start_date <= end_date:
            day = start_date.day
            month = start_date.month
            days_months.append((day, month))
            start_date += datetime.timedelta(days=1)
        for day, month in reversed(days_months):
            logging.info("Running corona_data_collector day=%s month=%s" %(day, month))
            try:
                run_log_main(parameters, run_row, False, {'source': 'db', 'day': day, 'month': month})
            except Exception as e:
                logging.exception(str(e))
                run_row['exception'] = 'exception for %s/%s: ' % (day, month) + str(e)
                return
            if not parameters.get('with-history'):
                return

    def _updated_files_callback(row):
        updated_file_hashes[os.path.normpath(os.path.join(output_dir, 'destination_output', row['path']))] = row['hash']

    hash_updated_files(
        '%s/destination_output' % output_dir,
        '%s/updated_files' % output_dir,
        _run_callback,
        updated_files_callback=_updated_files_callback
    ).process()

    if run_row['exception']:
        destination_file_hash = ''
    else:
        destination_file_hash = get_hash(run_row['destination_filename'])

    run_row['destination_file_hash'] = destination_file_hash
    run_row['github_sha'] = get_github_sha()

    if not run_row['exception']:
        store_destination_output_package(output_dir)

    return run_row, run_row['exception'] if run_row['exception'] and parameters.get('raise-exceptions') else None


def flow(parameters, *_):
    output_dir = parameters.get('output-dir', 'data/corona_data_collector')
    return keep_last_runs_history(output_dir, corona_data_collector_main, output_dir, parameters)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow({'raise-exceptions': True, 'with-history': '--with-history' in sys.argv}).process()
