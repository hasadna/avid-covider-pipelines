from dataflows import Flow
from common import utils
import os
import logging
import sys


def flow(parameters, *_):
    if parameters['module'] == 'src.utils.get_raw_data' and not os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE'):
        raise Exception('missing GOOGLE_SERVICE_ACCOUNT_FILE environment variable, '
                        'this is required for running get_raw_data via pipelines')
    logging.info('Running COVID19-ISRAEL module %s' % parameters['module'])
    if utils.subprocess_call_log(['python', '-u', '-m', parameters['module']], cwd='../COVID19-ISRAEL') != 0:
        raise Exception('Failed to run module %s' % parameters['module'])
    return Flow()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('usage:\n  %s <MODULE_NAME>\n' % sys.argv[0])
        print('for example:\n  %s src.utils.get_raw_data' % sys.argv[0])
        exit(1)
    logging.basicConfig(level=logging.DEBUG)
    flow({'module': sys.argv[1]}).process()
