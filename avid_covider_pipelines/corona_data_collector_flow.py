import logging
from corona_data_collector.main import main
import os
from avid_covider_pipelines.utils import keep_last_runs_history, hash_updated_files, get_hash


def corona_data_collector_main(last_run_row, run_row, output_dir, parameters):
    updated_file_hashes = {}

    def _run_callback():
        os.makedirs('%s/destination_output' % output_dir, exist_ok=True)
        try:
            for k, v in main({'source': 'db'}).items():
                run_row[k] = v
        except Exception as e:
            if parameters.get('raise-exceptions'):
                raise
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

    if run_row['exception'] and parameters.get('raise-exceptions'):
        raise Exception(run_row['exception'])

    run_row['destination_file_hash'] = destination_file_hash
    return run_row


def flow(parameters, *_):
    output_dir = parameters.get('output-dir', 'data/corona_data_collector')
    return keep_last_runs_history(output_dir, corona_data_collector_main, output_dir, parameters)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow({'raise-exceptions': True}).process()
