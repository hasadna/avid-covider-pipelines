from avid_covider_pipelines import utils
import logging
import sys
import json
import hashlib
import os
from datapackage_pipelines_covid19israel import publish_external_sharing_packages
import traceback


def run_covid19_israel(parameters, run_row):
    run_row['github_sha1'] = globals().get('COVID19_ISRAEL_GITHUB_SHA1', '_')
    args = parameters.get('args')
    if not args:
        args = []
    cmd = ['python', '-u', '-m', parameters['module'], *args]
    # cmd = ['echo'] + cmd
    # cmd = ['bash', '-c', 'echo %s && false' % cmd]
    log_files_dir = os.path.join(parameters['output-dir'], 'log_files')
    os.makedirs(log_files_dir, exist_ok=True)
    log_filename = os.path.join(log_files_dir, '%s.log' % run_row['start_time'].strftime('%Y%m%dT%H%M%S'))
    if utils.subprocess_call_log(
            cmd,
            log_file=log_filename,
            cwd='../COVID19-ISRAEL'
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
