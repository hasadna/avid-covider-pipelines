from dataflows import Flow, update_resource, load
import logging
from corona_data_collector import config
from avid_covider_pipelines.utils import dump_to_path
from sqlalchemy import create_engine
import os


def flow(parameters, *_):

    def _rows_iterator():
        last_id = None
        load_from = parameters['dump_to_path']
        if load_from and os.path.exists(os.path.join(load_from, "datapackage.json")):
            logging.info("Loading from last load_from_db package: " + os.path.join(load_from, "datapackage.json"))
            for resource in Flow(load(os.path.join(load_from, "datapackage.json"))).datastream().res_iter:
                for row in resource:
                    last_id = row['id']
                    yield row
        engine = create_engine(
            "postgresql://{username}:{password}@{host}:5432/reports?sslmode=verify-ca&sslrootcert={sslrootcert}&sslcert={sslcert}&sslkey={sslkey}".format(
                **config.db_settings))
        engine.update_execution_options(stream_results=True)
        if last_id:
            logging.info("Loading from id %s" % last_id)
            where = " where id > %s" % last_id
        else:
            logging.info("Loading all records")
            where = ""
        for id, created, data in engine.execute("select id, created, data from reports%s order by id" % where):
            last_id = id
            yield {"id": id, "created": created, "data": data}
            if id % 100000 == 0:
                logging.info("Loading id: %s" % id)
        logging.info("last_id = %s" % last_id)

    return Flow(
        _rows_iterator(),
        update_resource(-1, name="db_data", path="db_data.csv", **{"dpp:streaming": True}),
        dump_to_path(parameters['dump_to_path'])
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({'dump_to_path': 'data/corona_data_collector/load_from_db'}).process()
