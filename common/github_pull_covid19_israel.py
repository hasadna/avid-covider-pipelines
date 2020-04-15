from dataflows import Flow
import os
import logging
from common import utils


def flow(*_):
    logging.info('Pulling latest code from COVID19-ISRAEL github repo')
    logging.info('COVID19_ISRAEL_REPOSITORY=%s' % os.environ['COVID19_ISRAEL_REPOSITORY'])
    logging.info('branch = master')
    if utils.subprocess_call_log(['git', 'pull', 'origin', 'master'], cwd='../COVID19-ISRAEL') != 0:
        raise Exception('Failed to git pull')
    return Flow()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
