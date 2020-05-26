from avid_covider_pipelines import utils
import logging
import sys
import json
import hashlib
import os
from datapackage_pipelines_covid19israel import publish_external_sharing_packages
import traceback
from dataflows import Flow, load
import datetime
from collections import defaultdict


def did_pipeline_complete_successfully_today(output_dir):
    if os.path.exists(os.path.join(output_dir, "runs_history", "datapackage.json")):
        stats = defaultdict(int)

        def _process_runs_history(rows):
            for row in rows:
                yield row
                try:
                    if row["error"] == "no" and row["start_time"].strftime("%Y-%m-%d") == datetime.datetime.now().strftime("%Y-%m-%d"):
                        stats["completed_successfully"] += 1
                except Exception:
                    pass

        Flow(load(os.path.join(output_dir, "runs_history", "datapackage.json")), _process_runs_history).process()
        if stats["completed_successfully"] > 0:
            logging.info("pipeline completed successfully today")
            return True
        else:
            logging.info("pipeline did not complete successfully today")
            return False
    else:
        logging.info("pipeline does not have runs_history datapackage, this is probably the first run for this pipeline")
        return False


def run_covid19_israel(parameters, run_row):
    run_row['github_sha1'] = globals().get('COVID19_ISRAEL_GITHUB_SHA1', '_')
    log_files_dir = os.path.join(parameters['output-dir'], 'log_files')
    os.makedirs(log_files_dir, exist_ok=True)
    log_filename = os.path.join(log_files_dir, '%s.log' % run_row['start_time'].strftime('%Y%m%dT%H%M%S'))
    if parameters.get("run-once-daily") and did_pipeline_complete_successfully_today(parameters['output-dir']):
        run_row['error'] = 'yes'
        errmsg = "run-once-daily is set and pipeline already ran today"
        logging.error(errmsg)
        with open(log_filename, "w") as f:
            f.write(errmsg)
    else:
        args = parameters.get('args')
        if not args:
            args = []
        cmd = ['python', '-u', '-m', parameters['module'], *args]
        # cmd = ['echo'] + cmd
        # cmd = ['bash', '-c', 'echo %s && false' % cmd]
        if utils.subprocess_call_log(
                cmd,
                log_file=log_filename,
                cwd='../COVID19-ISRAEL',
                env={**os.environ, "COVID19_ISRAEL_GITHUB_SHA1": globals().get('COVID19_ISRAEL_GITHUB_SHA1', '_')}
        ) != 0:
            run_row['error'] = 'yes'
            logging.error('Failed to run COVID19-ISRAEL module %s with args %s' % (parameters['module'], args))
        else:
            run_row['error'] = 'no'
            external_sharing_packages = parameters.get("external_sharing_packages")
            if external_sharing_packages:
                try:
                    publish_external_sharing_packages.flow({"packages": external_sharing_packages}).process()
                except Exception:
                    errmsg = "Failed to export external sharing packages for module %s with args %s" % (parameters["module"], args)
                    with open(log_filename, "a") as f:
                        f.write(errmsg)
                        traceback.print_exc(file=f)
                    logging.exception(errmsg)
                    run_row['error'] = 'yes'


def flow(parameters, *_):
    logging.info('COVID19_ISRAEL_GITHUB_SHA1 = %s' % globals().get('COVID19_ISRAEL_GITHUB_SHA1', '_'))
    logging.info('Running COVID19-ISRAEL module %s with args %s' % (parameters['module'], parameters.get('args')))
    output_dir = parameters['output-dir']

    def _last_runs_run_callback(last_run_row, run_row):
        if parameters.get('datapackage-dependencies'):
            hasher = hashlib.sha256()
            for datapackage in parameters['datapackage-dependencies']:
               with open(datapackage) as f:
                   hasher.update(json.load(f)['hash'].encode())
            run_row['datapackage-dependencies-hash'] = hasher.hexdigest()
        else:
            run_row['datapackage-dependencies-hash'] = ''
        run_covid19_israel(parameters, run_row)
        return run_row, 'failed to run COVID19-ISRAEL module %s' % parameters if run_row['error'] == 'yes' and not parameters.get("skip-failures") else None

    return utils.keep_last_runs_history(output_dir, _last_runs_run_callback)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('usage:\n  %s <MODULE_NAME> [args..]\n' % sys.argv[0])
        print('for example:\n  %s src.utils.get_raw_data' % sys.argv[0])
        exit(1)
    logging.basicConfig(level=logging.DEBUG)
    module = sys.argv[1]
    flow({
        'module': module,
        'args': sys.argv[2:],
        'skip-failures': True,
        'external_sharing_packages': "FOOBAR",
        'output-dir': 'data/run_covid19_israel/%s' % module
    }).process()
