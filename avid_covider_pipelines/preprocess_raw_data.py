import logging
from avid_covider_pipelines.utils import keep_last_runs_history
from avid_covider_pipelines import github_pull_covid19_israel, run_covid19_israel
import os
from dataflows import Flow, load


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


def dump_last_run_logs(last_run_time):
    logging.info("Dumping last run logs (last_run_time=%s)" % last_run_time)
    for module in RUN_MODULES:
        log_file = 'data/preprocess_raw_data/log_files/%s/%s.log' % (
            module['id'], last_run_time.strftime('%Y%m%dT%H%M%S'))
        logging.info('log_file=%s' % log_file)
        if os.path.exists(log_file):
            with open(log_file) as f:
                for line in f:
                    logging.info(line.strip())
    return None


def preprocess_raw_data(last_run_row, run_row, parameters):
    last_run_sha1 = last_run_row.get('COVID19-ISRAEL_github_sha1')
    last_run_time = last_run_row.get('start_time')
    logging.info('last_run_sha1=%s last_run_time=%s' % (last_run_sha1, last_run_time))
    last_run_cdc_filename = last_run_row.get('cdc_filename')
    last_run_cdc_filehash = last_run_row.get('cdc_filehash')
    logging.info('last_run_cdc_filename=%s last_run_cdc_filehash=%s' % (last_run_cdc_filename, last_run_cdc_filehash))
    if last_run_time and (run_row['start_time'] - last_run_time).total_seconds() < 120:
        logging.info('last run was less then 120 seconds ago, not running')
        return dump_last_run_logs(last_run_time)
    new_sha1 = github_pull_covid19_israel.flow({
        'dump_to_path': '%s/last_github_pull' % OUTPUT_DIR
    }).results()[0][0][0]['sha1']
    logging.info('new_sha1=%s' % new_sha1)
    if os.path.exists('data/corona_data_collector/last_run/datapackage.json'):
        cdc_last_run = Flow(load('data/corona_data_collector/last_run/datapackage.json')).results()[0][0][0]
    else:
        cdc_last_run = {}
    cdc_filename = cdc_last_run.get('destination_filename')
    cdc_filehash = cdc_last_run.get('destination_file_hash')
    logging.info('cdc_filename=%s cdc_filehash=%s' % (cdc_filename, cdc_filehash))
    if last_run_sha1 and last_run_sha1 == new_sha1:
        logging.info("No change detected in COVID19-ISRAEL GitHub")
        if last_run_cdc_filename == cdc_filename:
            logging.info("No change detected in corona data collector filename")
            if last_run_cdc_filehash == cdc_filehash:
                logging.info("No change detected in corona data collector file hash")
                return dump_last_run_logs(last_run_time)
            else:
                logging.info('Change detected in corona data collector file hash')
        else:
            logging.info('Change detected in corona data collector filename')
    else:
        logging.info('Change detected in COVID19-ISRAEL GitHub')
    run_row['COVID19-ISRAEL_github_sha1'] = new_sha1
    run_row['cdc_filename'] = cdc_filename
    run_row['cdc_filehash'] = cdc_filehash
    for module in RUN_MODULES:
        try:
            os.makedirs('data/preprocess_raw_data/log_files/%s' % module['id'], exist_ok=True)
            run_covid19_israel.flow({
                'module': module['module'],
                'resource_name': '%s_last_updated_files' % module['id'],
                'dump_to_path': 'data/preprocess_raw_data/last_updated_files/%s' % module['id'],
                'log_file': 'data/preprocess_raw_data/log_files/%s/%s.log' % (
                module['id'], run_row['start_time'].strftime('%Y%m%dT%H%M%S'))
            }).process()
            run_row['%s_success' % module['id']] = 'yes'
        except Exception:
            logging.exception('failed to run %s' % module['id'])
            run_row['%s_success' % module['id']] = 'no'
    return run_row


def flow(parameters, *_):
    return keep_last_runs_history(OUTPUT_DIR, preprocess_raw_data, parameters)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow({}).process()
