import logging
import os
from dataflows import Flow, printer, load, update_resource
from avid_covider_pipelines.flows import github_pull_covid19_israel, run_covid19_israel


def load_if_exists(load_source, name, not_exists_rows, *args, **kwargs):
    if os.path.exists(load_source):
        return Flow(load(load_source, name, *args, **kwargs))
    else:
        return Flow(iter(not_exists_rows), update_resource(-1, name=name))



def flow(*_):
    return Flow(
        load_if_exists('data/github_pull_covid19_israel/datapackage.json', 'github_pull_covid19_israel', [{'sha1': ''}]),
        printer()
        # github_pull_covid19_israel.flow(),
        # run_covid19_israel.flow({'module': 'src.utils.get_raw_data'}),
        # run_covid19_israel.flow({'module': 'src.utils.preprocess_raw_data'}),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
