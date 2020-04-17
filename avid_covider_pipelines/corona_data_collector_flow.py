import logging
from corona_data_collector.main import main
import os
from avid_covider_pipelines.utils import keep_last_runs_history


OUTPUT_DIR = 'data/corona_data_collector'


def corona_data_collector_main(last_run_row, run_row):
    os.makedirs('%s/destination_output' % OUTPUT_DIR, exist_ok=True)
    for k, v in main({'source': 'db'}).items():
        run_row[k] = v
    return run_row


def flow(*_):
    return keep_last_runs_history(OUTPUT_DIR, corona_data_collector_main)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
