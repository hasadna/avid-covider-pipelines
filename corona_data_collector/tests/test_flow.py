import os
from dataflows import Flow, load
import tempfile
from corona_data_collector import add_gps_coordinates, export_corona_bot_answers
import logging
import random
import subprocess
from avid_covider_pipelines import utils


DOMAIN = os.environ["AVIDCOVIDER_PIPELINES_DATA_DOMAIN"]
AUTH_USER, AUTH_PASSWORD = os.environ["AVIDCOVIDER_PIPELINES_AUTH"].split(" ")


def main():
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, ".netrc"), "w") as f:
            f.write("machine %s\nlogin %s\npassword %s\n" % (DOMAIN, AUTH_USER, AUTH_PASSWORD))
        HOME = os.environ["HOME"]
        os.environ["HOME"] = tempdir
        os.makedirs("data/corona_data_collector/add_gps_coordinates/gps_data", exist_ok=True)
        utils.http_stream_download("data/corona_data_collector/add_gps_coordinates/gps_data/datapackage.json", {
            "url": "https://%s/data/corona_data_collector/add_gps_coordinates/gps_data/datapackage.json" % DOMAIN})
        utils.http_stream_download("data/corona_data_collector/add_gps_coordinates/gps_data/gps_data.csv", {
            "url": "https://%s/data/corona_data_collector/add_gps_coordinates/gps_data/gps_data.csv" % DOMAIN})
        Flow(
            load("https://%s/data/corona_data_collector/load_from_db/datapackage.json" % DOMAIN),
            add_gps_coordinates.flow({
                "gps_datapackage_path": "data/corona_data_collector/add_gps_coordinates/gps_data",
                "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
            }),
            export_corona_bot_answers.flow({
                "destination_output": "data/corona_data_collector/destination_output"
            })
        ).process()
    os.environ["HOME"] = HOME
    subprocess.check_call(["python3", "-m", "src.utils.get_raw_data"], cwd="../COVID19-ISRAEL", env={
        **os.environ,
        "GOOGLE_SERVICE_ACCOUNT_FILE": os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"],
        "AVIDCOVIDER_LOCAL_PATH": os.getcwd()
    })
    subprocess.check_call(["python3", "-m", "src.utils.preprocess_raw_data"], cwd="../COVID19-ISRAEL", env={
        **os.environ,
        "GOOGLE_API_KEY_FILE": os.environ["GOOGLE_API_KEY_FILE"]
    })
    logging.info("Great Success!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
