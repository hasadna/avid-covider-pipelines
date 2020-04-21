from dataflows import Flow, update_resource, printer
from avid_covider_pipelines.utils import dump_to_path
import os
import logging
from glob import glob
import datetime
import subprocess


IGNORE_FILENAME_STARTSWITH = [
    'venv',
]
IGNORE_FILENAME_CONTAINS = [
    'credentials',
    'google_api_key',
]
IGNORE_FILENAME_ENDSWITH = [
    '.pyc',
]


def is_ignore_filename(filename):
    for v in IGNORE_FILENAME_STARTSWITH:
        if filename.startswith(v):
            return True
    for v in IGNORE_FILENAME_CONTAINS:
        if v in filename:
            return True
    for v in IGNORE_FILENAME_ENDSWITH:
        if filename.endswith(v):
            return True
    return False


def get_git_filenames():
    output = subprocess.check_output(["git", "ls-tree", "-r", "HEAD", "--name-only"], cwd="../COVID19-ISRAEL")
    return set((line.strip() for line in output.decode().split("\n") if len(line.strip()) > 0))


def files_list(git_filenames=None):
    if not git_filenames:
        git_filenames = get_git_filenames()
    for filename in glob("../COVID19-ISRAEL/**", recursive=True):
        if not os.path.isfile(filename): continue
        rel_filename = os.path.relpath(filename, "../COVID19-ISRAEL/")
        if rel_filename in git_filenames: continue
        if is_ignore_filename(rel_filename): continue
        yield {
            "name": rel_filename,
            "size": os.path.getsize(filename),
            "mtime": datetime.datetime.fromtimestamp(os.path.getmtime(filename))
        }


def git_files_list(git_filenames):
    for filename in glob("../COVID19-ISRAEL/**", recursive=True):
        if not os.path.isfile(filename): continue
        rel_filename = os.path.relpath(filename, "../COVID19-ISRAEL/")
        if rel_filename not in git_filenames: continue
        yield {
            "name": rel_filename,
            "size": os.path.getsize(filename),
            "mtime": datetime.datetime.fromtimestamp(os.path.getmtime(filename))
        }


def flow(parameters, *_):
    logging.info('Generating files list from COVID19-ISRAEL directory')
    git_filenames = get_git_filenames()
    return Flow(
        git_files_list(git_filenames),
        update_resource(-1, name='git_files_list', path='git_files_list.csv', **{"dpp:streaming": True}),
        files_list(git_filenames),
        update_resource(-1, name='files_list', path='files_list.csv', **{"dpp:streaming": True}),
        dump_to_path(parameters['dump_to_path']),
        printer(num_rows=5)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    flow({
        "dump_to_path": "data/covid19_israel_files_list"
    }).process()
