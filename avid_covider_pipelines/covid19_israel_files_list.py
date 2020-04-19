from dataflows import Flow, update_resource, printer
from avid_covider_pipelines.utils import dump_to_path, is_ignore_hash_filename
import os
import logging
from glob import glob
import datetime


def files_list():
    for filename in glob("../COVID19-ISRAEL/**", recursive=True):
        if os.path.isfile(filename) and not is_ignore_hash_filename(filename):
            yield {
                "name": os.path.relpath(filename, "../COVID19-ISRAEL/"),
                "size": os.path.getsize(filename),
                "mtime": datetime.datetime.fromtimestamp(os.path.getmtime(filename))
            }


def flow(parameters, *_):
    logging.info('Generating files list from COVID19-ISRAEL directory')
    return Flow(
        files_list(),
        update_resource(-1, name='files_list', path='files_list.csv', **{"dpp:streaming": True}),
        dump_to_path(parameters.get('dump_to_path', "data/covid19_israel_files_list")),
        printer(num_rows=5)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    flow({}).process()
