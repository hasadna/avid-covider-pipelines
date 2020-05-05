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


def _mock_gender_other(rows):
    if rows.res.name == "db_data":
        logging.info("Mocking sex 'other' for ids 640000 to 640100")
        for row in rows:
            if 640000 <= int(row["__id"]) <= 640100:
                row["sex"] = '"other"'
            yield row
    else:
        for row in rows:
            yield row


def _mock_version_28(id, created, data):
    if 640000 <= id <= 640020:
        logging.info("Mocking version 2.8 for ids 640000 to 640020: "
                     "assisted_living = no_response , "
                     "abdominal_pain = true , "
                     "lack_of_appetite_or_skipping_meals = false , "
                     "work_outside = no")
        data["assisted_living"] = "no_response"
        data["abdominal_pain"] = True
        data["lack_of_appetite_or_skipping_meals"] = False
        data["work_outside"] = False
        data["version"] = "2.8.0"
    elif 640021 <= id <= 640040:
        logging.info("Mocking version 2.8 for ids 640021 to 640040: "
                     "assisted_living = true , "
                     "abdominal_pain = false , "
                     "lack_of_appetite_or_skipping_meals = true , "
                     "work_outside = yes + full details")
        data["assisted_living"] = True
        data["abdominal_pain"] = False
        data["lack_of_appetite_or_skipping_meals"] = True
        data["work_outside"] = True
        data["work_outside_avg_weekly_hours"] = 3
        data["work_outside_city_town"] = "תל אביב"
        data["work_outside_street"] = "הרצל"
        data["version"] = "2.8.0"
    elif 640041 <= id <= 640060:
        logging.info("Mocking version 2.8 for ids 640041 to 640060: "
                     "assisted_living = false , "
                     "work_outside = yes + minimal details")
        data["assisted_living"] = False
        data["work_outside"] = True
        data["work_outside_avg_weekly_hours"] = 55
        data["version"] = "2.8.0"
    elif 640061 <= id <= 640080:
        logging.info("Mocking version 2.8 for ids 640061 to 640080: "
                     "assisted_living = 'true'")
        data["assisted_living"] = "true"
        data["version"] = "2.8.0"
    elif 640081 <= id <= 640100:
        logging.info("Mocking version 2.8 for ids 640081 to 640100: "
                     "assisted_living = 'false'")
        data["assisted_living"] = "false"
        data["version"] = "2.8.0"
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
                "limit_rows": 50000,
                "files_dump_to_path": "data/corona_data_collector/gdrive_data",
                "google_drive_csv_folder_id": "1pzAyk-uXy__bt1tCX4rpTiPZNmrehTOz",
                "file_sources": {
                    "COVID-19-English.csv": "google",
                    "COVID-19-Russian.csv": "google",
                    "COVID-19-Hebrew.csv": "hebrew_google",
                    "maccabi_updated.csv": "maccabi",
                }
            }),
            load_from_db.flow({
                "where": "(id > 500 and id < 1000) or (id > 180000 and id < 185000) or (id > 600000 and id < 601000) or (id > 640000 and id < 641000) or (id > 670000)",
                "filter_db_row_callback": _mock_version_28
            }),
            _mock_gender_other,
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
