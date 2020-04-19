from dataflows import Flow, update_resource, printer
from avid_covider_pipelines.utils import dump_to_path
import os
import logging
from avid_covider_pipelines import utils
import subprocess


def flow(parameters, *_):
    logging.info('Pulling latest code from COVID19-ISRAEL github repo')
    logging.info('COVID19_ISRAEL_REPOSITORY=%s' % os.environ.get('COVID19_ISRAEL_REPOSITORY'))
    logging.info('COVID19_ISRAEL_BRANCH=%s' % os.environ.get('COVID19_ISRAEL_BRANCH'))
    utils.subprocess_call_log(['git', 'config', 'user.email', 'avid-covider-pipelines@localhost'], cwd='../COVID19-ISRAEL')
    utils.subprocess_call_log(['git', 'config', 'user.name', 'avid-covider-pipelines'], cwd='../COVID19-ISRAEL')
    branch = os.environ.get('COVID19_ISRAEL_BRANCH')
    if branch:
        logging.info('Pulling from origin/' + branch)
        if utils.subprocess_call_log(['git', 'fetch', 'origin'], cwd='../COVID19-ISRAEL') != 0:
            raise Exception('Failed to fetch origin')
        if utils.subprocess_call_log(['git', 'checkout', branch], cwd='../COVID19-ISRAEL') != 0:
            raise Exception('Failed to switch branch')
        if utils.subprocess_call_log(['git', 'pull', 'origin', branch], cwd='../COVID19-ISRAEL') != 0:
            raise Exception('Failed to git pull')
    else:
        logging.info('pulling from origin/master')
        if utils.subprocess_call_log(['git', 'pull', 'origin', 'master'], cwd='../COVID19-ISRAEL') != 0:
            raise Exception('Failed to git pull')
    sha1 = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd='../COVID19-ISRAEL').decode().strip()
    # sha1 = subprocess.check_output(['cat', '/pipelines/data/fake-sha1'], cwd='../COVID19-ISRAEL').decode().strip()
    return Flow(
        iter([{'sha1': sha1}]),
        update_resource(-1, name='github_pull_covid19_israel', path='github_pull_covid19_israel.csv', **{'dpp:streaming': True}),
        printer(),
        dump_to_path(parameters.get('dump_to_path', 'data/github_pull_covid19_israel'))
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    flow({}).process()
