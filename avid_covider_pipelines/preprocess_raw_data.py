import logging
from avid_covider_pipelines.utils import keep_last_runs_history
from avid_covider_pipelines import github_pull_covid19_israel, run_covid19_israel
import datetime
import os


OUTPUT_DIR = 'data/preprocess_raw_data'
RUN_MODULES = [
    {
        'id': 'get_raw_data',
        'module': 'src.utils.get_raw_data',
    },
    {
        'id': 'preprocess_raw_data',
        'module': 'src.utils.preprocess_raw_data',
    },
]


def preprocess_raw_data(last_run_row, run_row):
    last_run_sha1 = last_run_row.get('COVID19-ISRAEL_github_sha1')
    last_run_time = last_run_row.get('start_time')
    if last_run_time and (datetime.datetime.now() - last_run_time).total_seconds() < 120:
        logging.info('last run was less then 120 seconds ago, not running')
        return None
    new_sha1 = github_pull_covid19_israel.flow({
        'dump_to_path': '%s/last_github_pull' % OUTPUT_DIR
    }).results()[0][0][0]['sha1']
    if (
            last_run_time and (
            datetime.datetime.now() - last_run_time).total_seconds() < 60 * 60 * 24
            and last_run_sha1 == new_sha1
    ):
        logging.info("No change detected in COVID19-ISRAEL GitHub, not running")
        return None
    run_row['COVID19-ISRAEL_github_sha1'] = new_sha1
    for module in RUN_MODULES:
        try:
            os.makedirs('data/preprocess_raw_data/log_files/%s' % module['id'], exist_ok=True)
            run_covid19_israel.flow({
                'module': module['module'],
                'resource_name': '%s_last_updated_files' % module['id'],
                'dump_to_path': 'data/preprocess_raw_data/last_updated_files/%s' % module['id'],
                'log_file': 'data/preprocess_raw_data/log_files/%s/%s.log' % (
                module['id'], datetime.datetime.now().strftime('%Y%m%dT%H%M%S'))
            }).process()
            run_row['%s_success' % module['id']] = 'yes'
        except Exception:
            logging.exception('failed to run %s' % module['id'])
            run_row['%s_success' % module['id']] = 'no'
    return run_row


def flow(*_):
    return keep_last_runs_history(OUTPUT_DIR, preprocess_raw_data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
