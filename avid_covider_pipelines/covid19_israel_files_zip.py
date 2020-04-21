from dataflows import Flow, update_resource, printer, load
from avid_covider_pipelines.utils import dump_to_path
from avid_covider_pipelines.covid19_israel_files_list import files_list
import logging
import zipfile
import os


def zip_files(zipf):
    for file in files_list():
        zipf.write(os.path.join("..", "COVID19-ISRAEL", file['name']), file['name'])
        yield file


def flow(parameters, *_):
    logging.info('Creating COVID19-ISRAEL files zip')
    os.makedirs(parameters["dump_to_path"], exist_ok=True)
    with zipfile.ZipFile(os.path.join(parameters["dump_to_path"], "covid19-israel-data.zip"), "w", zipfile.ZIP_LZMA) as zipf:
        Flow(
            zip_files(zipf),
            update_resource(-1, name='files_list', path='files_list.csv', **{"dpp:streaming": True}),
            dump_to_path(parameters['dump_to_path']),
        ).process()
    return Flow(
        load(os.path.join(parameters['dump_to_path'], "datapackage.json")),
        printer(num_rows=5)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    flow({
        "dump_to_path": "data/covid19_israel_files_zip"
    }).process()
