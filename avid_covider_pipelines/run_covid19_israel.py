from avid_covider_pipelines import utils
import logging
import sys
import json
import hashlib
import os


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
    if utils.subprocess_call_log(
            cmd,
            log_file=os.path.join(log_files_dir, '%s.log' % run_row['start_time'].strftime('%Y%m%dT%H%M%S')),
            cwd='../COVID19-ISRAEL'
    ) != 0:
        run_row['error'] = 'yes'
        logging.error('Failed to run COVID19-ISRAEL module %s with args %s' % (parameters['module'], args))
    else:
        run_row['error'] = 'no'


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
        return run_row, 'failed to run COVID19-ISRAEL module' if run_row['error'] == 'yes' and parameters.get('raise-exceptions') else None

    def _hash_updated_files_run_callback():
        utils.keep_last_runs_history(
            output_dir, _last_runs_run_callback
        ).process()

    return utils.hash_updated_files(
        '../COVID19-ISRAEL',
        '%s/last_updated_files' % output_dir,
        _hash_updated_files_run_callback,
        printer_num_rows=parameters.get('printer_num_rows', 10),
    )



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
        'raise-exceptions': True,
        'output-dir': 'data/run_covid19_israel/%s' % module
    }).process()
