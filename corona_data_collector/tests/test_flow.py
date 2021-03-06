import os
from dataflows import Flow
import tempfile
from corona_data_collector import add_gps_coordinates, export_corona_bot_answers, load_from_db, download_gdrive_data
import logging
import random
import subprocess
from avid_covider_pipelines import utils


DOMAIN = os.environ["AVIDCOVIDER_PIPELINES_DATA_DOMAIN"]
AUTH_USER, AUTH_PASSWORD = os.environ["AVIDCOVIDER_PIPELINES_AUTH"].split(" ")


# def _mock_school_name(id, created, data):
#     try:
#         age = int(data["age"])
#     except Exception:
#         age = None
#     if 640000 <= id <= 640100:
#         data["version"] = "3.1.0"
#         if age and age < 18:
#             agemsg = "with school_name (for age<18)"
#             data["school_name"] = "הבית ספר של נווה חמציצים"
#         else:
#             agemsg = ""
#             data["school_name"] = ""
#         logging.info("Mocking version 3.1 for ids 640000 to 640100 %s" % agemsg)
#     return id, created, data


# def _mock_symptoms_duration(id, created, data):
#     if id in (851523, 850724, 849379):
#         data["version"] = "4.1.0"
#     return id, created, data


def _mock_hospitalization_icu(id, created, data):
    if id == 854455:
        data["hospitalization_icu_required"] = True
        data["hospitalization_icu_duration"] = "5"
    return id, created, data


def _filter_db_row_callback(id, created, data):
    # id, created, data = _mock_school_name(id, created, data)
    # id, created, data = _mock_symptoms_duration(id, created, data)
    id, created, data = _mock_hospitalization_icu(id, created, data)
    return id, created, data


def main():
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, ".netrc"), "w") as f:
            f.write("machine %s\nlogin %s\npassword %s\n" % (DOMAIN, AUTH_USER, AUTH_PASSWORD))
        HOME = os.environ["HOME"]
        os.environ["HOME"] = tempdir
        os.makedirs("data/corona_data_collector/gps_data_cache", exist_ok=True)
        utils.http_stream_download("data/corona_data_collector/gps_data_cache/datapackage.json", {
            "url": "https://%s/data/corona_data_collector/gps_data_cache/datapackage.json" % DOMAIN})
        utils.http_stream_download("data/corona_data_collector/gps_data_cache/gps_data.csv", {
            "url": "https://%s/data/corona_data_collector/gps_data_cache/gps_data.csv" % DOMAIN})
        Flow(
            download_gdrive_data.flow({
                "limit_rows": 5000,
                "files_dump_to_path": "data/corona_data_collector/gdrive_data",
                "google_drive_csv_folder_id": "1pzAyk-uXy__bt1tCX4rpTiPZNmrehTOz",
                "file_sources": {
                    "COVID-19-English.csv": "google",
                    "COVID-19-Russian.csv": "google",
                    "COVID-19-Hebrew.csv": "hebrew_google",
                }
            }),
            load_from_db.flow({
                "where": "   (id > 500    and id < 1000  ) "
                         "or (id > 180000 and id < 185000) "
                         "or (id > 321000 and id < 322000) "
                         "or (id > 462000 and id < 463000) "
                         "or (id > 600000 and id < 601000) "
                         "or (id > 640000 and id < 641000) "
                         "or (id > 670000 and id < 670500) "
                         "or (id < 849000 and id < 855000) "
                         "or (id > 860000 and id < 865000) ",
                "filter_db_row_callback": _filter_db_row_callback
            }),
            add_gps_coordinates.flow({
                "source_fields": utils.get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
                "workplace_source_fields": utils.get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["workplace_source_fields"],
                "dump_to_path": "data/corona_data_collector/with_gps_data",
                "gps_datapackage_path": "data/corona_data_collector/gps_data_cache",
                "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
            }),
            export_corona_bot_answers.flow({
                "destination_output": "data/corona_data_collector/corona_bot_answers"
            }),
            export_corona_bot_answers.flow({
                "unsupported": True,
                "destination_output": "data/corona_data_collector/corona_bot_answers_unsupported"
            })
        ).process()
    os.environ["HOME"] = HOME
    subprocess.check_call(["python3", "-m", "src.utils.get_raw_data"], cwd="../COVID19-ISRAEL", env={
        **os.environ,
        "GOOGLE_SERVICE_ACCOUNT_FILE": os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"],
        "AVIDCOVIDER_LOCAL_PATH": os.getcwd()
    })
    subprocess.check_call(["python3", "-m", "src.utils.preprocess_raw_data"], cwd="../COVID19-ISRAEL", env={
        **os.environ
    })
    logging.info("Great Success!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
