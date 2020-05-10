import logging
from dataflows import Flow, load, printer, update_resource


def flow(parameters, *_):

    def _get_last_runs():
        for id, path in parameters["check_covid19_israel_id_paths"].items():
            last_run = Flow(load("%s/last_run/datapackage.json" % path)).results()[0][0][0]
            last_run = {"id": id, "github_sha1": last_run.get("github_sha1", "_"), "error": last_run.get("error", "yes")}
            yield last_run

    def _check_last_runs(rows):
        has_errors = []
        for row in rows:
            yield row
            if row["error"] != "no":
                has_errors.append(row["id"])
        if len(has_errors) > 0:
            raise Exception("pipelines failed: %s" % has_errors)

    return Flow(
        _get_last_runs(),
        update_resource(-1, name="last_runs", path="last_runs.csv", **{"dpp:streaming": True}),
        printer(num_rows=9999),
        _check_last_runs,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "check_covid19_israel_id_paths": {
            "get_raw_data": "data/run_covid19_israel/src.utils.get_raw_data",
            "preprocess_raw_data": "data/preprocess_raw_data",
        }
    }).process()
