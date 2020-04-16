import logging
from dataflows import Flow, printer, set_type, update_resource, load, sort_rows
from avid_covider_pipelines.utils import dump_to_path
from avid_covider_pipelines.utils import load_if_exists
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


def flow(*_):
    run_row = None
    last_run_row = Flow(load_if_exists('%s/last_run/datapackage.json' % OUTPUT_DIR, 'last_run', [{}])).results()[0][0][0]
    last_run_sha1 = last_run_row.get('COVID19-ISRAEL_github_sha1')
    last_run_time = last_run_row.get('start_time')
    if last_run_time and (datetime.datetime.now() - last_run_time).total_seconds() < 120:
        logging.info('last run was less then 120 seconds ago, not running')
    else:
        new_sha1 = github_pull_covid19_israel.flow({
            'dump_to_path': '%s/last_github_pull' % OUTPUT_DIR
        }).results()[0][0][0]['sha1']
        if last_run_time and (datetime.datetime.now() - last_run_time).total_seconds() < 60*60*24 and last_run_sha1 == new_sha1:
            logging.info("No change detected in COVID19-ISRAEL GitHub, not running")
        else:
            run_row = {'start_time': datetime.datetime.now(), 'COVID19-ISRAEL_github_sha1': new_sha1}
            for module in RUN_MODULES:
                try:
                    os.makedirs('data/preprocess_raw_data/log_files/%s' % module['id'], exist_ok=True)
                    run_covid19_israel.flow({
                        'module': module['module'],
                        'resource_name': '%s_last_updated_files' % module['id'],
                        'dump_to_path': 'data/preprocess_raw_data/last_updated_files/%s' % module['id'],
                        'log_file': 'data/preprocess_raw_data/log_files/%s/%s.log' % (module['id'],datetime.datetime.now().strftime('%Y%m%dT%H%M%S'))
                    }).process()
                    run_row['%s_success' % module['id']] = 'yes'
                except Exception:
                    logging.exception('failed to run %s' % module['id'])
                    run_row['%s_success' % module['id']] = 'no'

    if run_row is not None:
        Flow(
            iter([run_row]),
            update_resource(-1, name='last_run', path='last_run.csv', **{'dpp:streaming': True}),
            dump_to_path('%s/last_run' % OUTPUT_DIR)
        ).process()

    def _get_runs_history():
        if os.path.exists('%s/runs_history/datapackage.json' % OUTPUT_DIR):
            for resource in Flow(
                load('%s/runs_history/datapackage.json' % OUTPUT_DIR),
            ).datastream().res_iter:
                yield from resource
        if run_row is not None:
            yield run_row

    Flow(
        _get_runs_history(),
        update_resource(-1, name='runs_history', path='runs_history', **{'dpp:streaming': True}),
        dump_to_path('%s/runs_history' % OUTPUT_DIR)
    ).process()

    return Flow(
        load('%s/runs_history/datapackage.json' % OUTPUT_DIR),
        sort_rows('{start_time}', reverse=True),
        printer(num_rows=10)
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
