import logging
import datetime
from dataflows import Flow, load, printer, update_resource
from avid_covider_pipelines.utils import dump_to_path


def flow(parameters, *_):

    def _get_last_runs():
        runs_history_last_rows = {}
        for id, path in parameters["check_covid19_israel_id_paths"].items():

            def _process_runs_history(rows):
                for row in rows:
                    yield row
                    runs_history_last_rows[id] = row

            Flow(load("%s/runs_history/datapackage.json" % path), _process_runs_history).process()
        for id, row in runs_history_last_rows.items():
            start_time = row["start_time"]
            end_time = datetime.datetime.strptime(row["end_time"], '%Y-%m-%dT%H:%M:%S')
            yield {
                "id": id,
                "github_sha1": row["github_sha1"],
                "error": row["error"],
                "start_time": start_time,
                "end_time": end_time,
                "duration_minutes": (end_time - start_time).total_seconds() / 60,
                "log_file": "https://avidcovider-pipelines-data.odata.org.il/data/%s/log_files/%s.log" % (id, start_time.strftime("%Y%m%dT%H%M%S")),
            }

    def _check_last_runs(rows):
        has_errors = []
        for row in rows:
            yield row
            if row["error"] != "no":
                has_errors.append(row["id"])
        if len(has_errors) > 0:
            raise Exception("pipelines failed: %s" % has_errors)

    Flow(
        _get_last_runs(),
        update_resource(-1, name="last_runs", path="last_runs.csv", schema={
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "github_sha1", "type": "string"},
                {"name": "error", "type": "string"},
                {"name": "start_time", "type": "datetime"},
                {"name": "end_time", "type": "datetime"},
                {"name": "duration_minutes", "type": "number"},
                {"name": "log_file", "type": "string"},
            ]
        }, **{"dpp:streaming": True}),
        printer(num_rows=9999),
        dump_to_path(parameters["output-dir"]),
    ).process()
    return Flow(
        load("%s/datapackage.json" % parameters["output-dir"]),
        _check_last_runs
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "check_covid19_israel_id_paths": {
            "get_raw_data": "data/run_covid19_israel/src.utils.get_raw_data",
            "preprocess_raw_data": "data/preprocess_raw_data",
        }
    }).process()
