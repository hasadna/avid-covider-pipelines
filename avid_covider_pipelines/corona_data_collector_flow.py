import logging
from dataflows import Flow, update_resource
from corona_data_collector.main import main
import os


def flow(*_):
    os.makedirs('data/corona_data_collector/destination_output', exist_ok=True)
    main({'source': 'db'})
    return Flow(iter([{'ok': 'yes'}]), update_resource(-1, **{'dpp:streaming': True}))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flow().process()
