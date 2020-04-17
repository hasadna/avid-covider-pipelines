import logging
from corona_data_collector.main import main
import os
from avid_covider_pipelines.utils import keep_last_runs_history, hash_updated_files, get_hash


OUTPUT_DIR = 'data/corona_data_collector'


def corona_data_collector_main(last_run_row, run_row):
    updated_file_hashes = {}

    def _run_callback():
        os.makedirs('%s/destination_output' % OUTPUT_DIR, exist_ok=True)
        for k, v in main({'source': 'db'}).items():
            run_row[k] = v

    def _updated_files_callback(row):
        updated_file_hashes[os.path.normpath(os.path.join(OUTPUT_DIR, 'destination_output', row['path']))] = row['hash']

    hash_updated_files(
        '%s/destination_output' % OUTPUT_DIR,
        '%s/updated_files' % OUTPUT_DIR,
        _run_callback,
        updated_files_callback=_updated_files_callback
    ).process()

    if len(updated_file_hashes) > 1:
        raise Exception('Too many files changed by corona_data_collector')
    elif len(updated_file_hashes) == 1:
        destination_file_hash = updated_file_hashes.get(os.path.normpath(run_row['destination_filename']))
        destination_file_is_changed = True
        if not destination_file_hash:
            raise Exception("Failed to get updated hash for corona_data_collector")
    else:
        destination_file_hash = get_hash(run_row['destination_filename'])
        destination_file_is_changed = False
    run_row['destination_file_hash'] = destination_file_hash
    run_row['destination_file_is_changed'] = 'yes' if destination_file_is_changed else 'no'
    return run_row


def flow(*_):
    return keep_last_runs_history(OUTPUT_DIR, corona_data_collector_main)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
