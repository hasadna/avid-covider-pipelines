from dataflows import Flow, update_resource, load
import logging
from corona_data_collector import config
from avid_covider_pipelines.utils import dump_to_path, get_parameters_from_pipeline_spec
from sqlalchemy import create_engine
import os
from kvfile import KVFile
from collections import defaultdict
import json


def flow(parameters, *_):
    stats = defaultdict(int)
    kv = KVFile()
    last_id = None
    load_from = parameters.get("load_from", parameters.get('dump_to_path'))
    if load_from and os.path.exists(os.path.join(load_from, "datapackage.json")):
        logging.info("Loading from last load_from_db package: " + os.path.join(load_from, "datapackage.json"))
        row = None
        for resource in Flow(load(os.path.join(load_from, "datapackage.json"), limit_rows=parameters.get("limit_rows"), resources="db_data")).datastream().res_iter:
            for row in resource:
                stats['loaded from package'] += 1
                last_id = row['__id']
                kv.set("{:0>12}".format(last_id), row)
                if last_id % 10000 == 0:
                    logging.info("Loaded id: %s" % last_id)
        all_data_keys = set(row.keys()) if row else set()
    else:
        all_data_keys = set()
    logging.info('num rows loaded from package: %s' % stats['loaded from package'])
    engine = create_engine(
        "postgresql://{username}:{password}@{host}:5432/reports?sslmode=verify-ca&sslrootcert={sslrootcert}&sslcert={sslcert}&sslkey={sslkey}".format(
            **config.db_settings))
    engine.update_execution_options(stream_results=True)
    if parameters.get("where"):
        logging.info("Loading from DB, with where clause: " + parameters["where"])
        where = " where " + parameters["where"]
    elif last_id:
        logging.info("Loading from DB, starting at id %s" % last_id)
        where = " where id > %s" % last_id
    else:
        logging.info("Loading all records from DB")
        where = ""
    for id, created, data in engine.execute("select id, created, data from reports%s order by id" % where):
        if not data or not isinstance(data, dict):
            stats['invalid data'] += 1
            continue
        stats['loaded from db'] += 1
        last_id = id
        row = {"__id": id, "__created": created,}
        for k, v in data.items():
            all_data_keys.add(k)
            row[k] = v
        kv.set("{:0>12}".format(id), row)
        if id % 100000 == 0:
            logging.info("Loaded id: %s" % id)
        if parameters.get("limit_rows") and stats['loaded from db'] > parameters["limit_rows"]:
            break
    logging.info('DB rows with invalid data: %s' % stats['invalid data'])
    logging.info("last_id = %s" % last_id)
    logging.info('num rows loaded from db: %s' % stats['loaded from db'])

    def _yield_from_kv():
        for _, row in kv.items():
            yield {
                "__id": row["__id"],
                "__created": row["__created"],
                **{
                    k: json.dumps(row.get(k)) for k in all_data_keys
                    if k not in ["__id", "__created"]
                }
            }

    flow_args = [
        _yield_from_kv(),
        update_resource(-1, name="db_data", path="db_data.csv", schema={
            "fields": [
                {"name": "__id", "type": "integer"},
                {"name": "__created", "type": "datetime"},
                *[
                    {"name": k, "type": "string"}
                    for k in all_data_keys if k not in ["__id", "__created"]
                ]
            ]
        }, **{"dpp:streaming": True}),
    ]
    if parameters.get("dump_to_path"):
        flow_args += [
            dump_to_path(parameters['dump_to_path'])
        ]
    return Flow(*flow_args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "limit_rows": 200,
        "dump_to_path": "data/corona_data_collector/load_from_db",
    }).process()
