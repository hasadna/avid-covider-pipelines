import logging
from dataflows import Flow, printer, dump_to_path, set_type, update_resource
from avid_covider_pipelines.utils import load_if_exists
from avid_covider_pipelines import github_pull_covid19_israel, run_covid19_israel
import datetime


def flow(*_):
    last_run_row = Flow(load_if_exists('data/preprocess_raw_data/last_run/datapackage.json', 'last_run', [{}])).results()[0][0][0]
    last_run_sha1 = last_run_row.get('COVID19-ISRAEL_github_sha1')
    last_run_time = last_run_row.get('start_time')
    if last_run_time and (datetime.datetime.now() - last_run_time).total_seconds() < 120:
        logging.info('last run was less then 120 seconds ago, not running')
        return Flow(iter([]))
    new_sha1 = github_pull_covid19_israel.flow({
        'dump_to_path': 'data/preprocess_raw_data/last_github_pull'
    }).results()[0][0][0]['sha1']
    if last_run_time and (datetime.datetime.now() - last_run_time).total_seconds() < 60*60*24:
        if last_run_sha1 == new_sha1:
            logging.info("No change detected in COVID19-ISRAEL GitHub, not running")
            return Flow(iter([]))
    else:
        logging.info('Last run was more then 24 hours ago, will re-run regardless of change in GitHub')
    run_row = {'COVID19-ISRAEL_github_sha1': new_sha1,
               'start_time': datetime.datetime.now()}
    try:
        run_covid19_israel.flow({
            'module': 'src.utils.get_raw_data',
            'resource_name': 'get_raw_data_last_updated_files',
            'dump_to_path': 'data/preprocess_raw_data/get_raw_data_last_updated_files',
        }).process()
        run_row['get_raw_data_success'] = 'true'
    except Exception:
        logging.exception('failed to run get_raw_data')
        run_row['get_raw_data_success'] = 'false'
    try:
        run_covid19_israel.flow({
            'module': 'src.utils.preprocess_raw_data',
            'resource_name': 'preprocess_raw_data_last_updated_files',
            'dump_to_path': 'data/preprocess_raw_data/preprocess_raw_data_last_updated_files',
        }).process()
        run_row['preprocess_raw_data_success'] = 'true'
    except Exception:
        logging.exception('failed to run get_raw_data')
        run_row['preprocess_raw_data_success'] = 'false'
    return Flow(
        iter([run_row]),
        update_resource(-1, name='last_run', path='last_run.csv', **{'dpp:streaming': True}),
        set_type('get_raw_data_success', type='boolean'),
        set_type('preprocess_raw_data_success', type='boolean'),
        printer(num_rows=999),
        dump_to_path('data/preprocess_raw_data/last_run')
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
