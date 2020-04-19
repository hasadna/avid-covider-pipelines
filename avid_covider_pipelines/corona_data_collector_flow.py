import logging
from corona_data_collector.main import main
import os
from avid_covider_pipelines.utils import keep_last_runs_history, hash_updated_files, get_hash, get_github_sha, dump_to_path
from glob import glob
import datetime
from dataflows import update_resource, Flow


def store_destination_output_package(output_dir):

    def _files_list():
        for filename in glob(os.path.join(output_dir, "destination_output", "*")):
            if os.path.isfile(filename):
                yield {
                    "name": os.path.relpath(filename, os.path.join(output_dir, "destination_output")),
                    "size": os.path.getsize(filename),
                    "mtime": datetime.datetime.fromtimestamp(os.path.getmtime(filename))
                }

    Flow(
        _files_list(),
        update_resource(-1, name='files_list', path='files_list.csv'),
        dump_to_path(os.path.join(output_dir, "destination_output")),
    ).process()


def corona_data_collector_main(last_run_row, run_row, output_dir, parameters):
    updated_file_hashes = {}

    def _run_callback():
        os.makedirs('%s/destination_output' % output_dir, exist_ok=True)
        try:
            for k, v in main({'source': 'db'}).items():
                run_row[k] = v
        except Exception as e:
            logging.exception(str(e))
            run_row['exception'] = str(e)
        else:
            run_row['exception'] = ''

    def _updated_files_callback(row):
        updated_file_hashes[os.path.normpath(os.path.join(output_dir, 'destination_output', row['path']))] = row['hash']

    hash_updated_files(
        '%s/destination_output' % output_dir,
        '%s/updated_files' % output_dir,
        _run_callback,
        updated_files_callback=_updated_files_callback
    ).process()

    destination_file_hash = ''
    if not run_row['exception']:
        if len(updated_file_hashes) > 1:
            run_row['exception'] = 'Too many files changed by corona_data_collector'
        elif len(updated_file_hashes) == 1:
            destination_file_hash = updated_file_hashes.get(os.path.normpath(run_row['destination_filename']))
            if not destination_file_hash:
                destination_file_hash = ''
                run_row['exception'] = "Failed to get updated hash for corona_data_collector"
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
    flow({'raise-exceptions': True}).process()
