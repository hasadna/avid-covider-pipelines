from avid_covider_pipelines import utils
import logging
import sys


def run_covid19_israel(parameters):
    if utils.subprocess_call_log(['python', '-u', '-m', parameters['module']], log_file=parameters.get('log_file'), cwd='../COVID19-ISRAEL') != 0:
        raise Exception('Failed to run COVID19-ISRAEL module %s' % parameters['module'])


def flow(parameters, *_):
    logging.info('Running COVID19-ISRAEL module %s' % parameters['module'])
    return utils.hash_updated_files(
        '../COVID19-ISRAEL',
        parameters.get('dump_to_path', 'data/run_covid19_israel/last_updated_files/%s' % parameters['module']),
        run_covid19_israel,
        printer_num_rows=parameters.get('printer_num_rows', 999),
        run_callback_args=[parameters]
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('usage:\n  %s <MODULE_NAME>\n' % sys.argv[0])
        print('for example:\n  %s src.utils.get_raw_data' % sys.argv[0])
        exit(1)
    logging.basicConfig(level=logging.DEBUG)
    flow({'module': sys.argv[1]}).process()
