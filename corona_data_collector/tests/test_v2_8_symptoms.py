import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


def _mock_abdominal_pain(id, created, data):
    if id == 600304:
        logging.info("Mocking version 2.8 for id 600304 with abdominal_pain = true , lack_of_appetite_or_skipping_meals = false")
        data["abdominal_pain"] = True
        data["lack_of_appetite_or_skipping_meals"] = False
        data["version"] = "2.8.0"
    elif id == 676580:
        logging.info("Mocking version 2.8 for id 676580 with abdominal_pain = false , lack_of_appetite_or_skipping_meals = true")
        data["abdominal_pain"] = False
        data["lack_of_appetite_or_skipping_meals"] = True
        data["version"] = "2.8.0"
    return id, created, data


Flow(
    load_from_db.flow({
        "where": "id in (94, 180075, 600304, 600895, 676580, 676581, 701508)",
        "filter_db_row_callback": _mock_abdominal_pain
    }),
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "version", "abdominal_pain", "lack_of_appetite_or_skipping_meals"
    ]),
).process()
Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_22_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_20_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_29_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_2_5_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: (str(row["symptom_abdominal_pain"]), str(row["symptom_lack_of_appetite_skipping_meals"]), str(row["questionare_version"])),
        {
            "94": ["corona_bot_answers_22_3_2020_with_coords", "", "", "0.1.0"],
            "180075": ["corona_bot_answers_25_3_2020_with_coords", "", "", "1.0.1"],
            "600304": ["corona_bot_answers_20_4_2020_with_coords", "1", "0", "2.8.0"],
            "600895": ["corona_bot_answers_20_4_2020_with_coords", "", "", "2.6.0"],
            "676580": ["corona_bot_answers_29_4_2020_with_coords", "0", "1", "2.8.0"],
            "676581": ["corona_bot_answers_29_4_2020_with_coords", "", "", "2.7.4"],
            "701508": ["corona_bot_answers_2_5_2020_with_coords", "", "", "2.7.6"],
        }
    ),
    printer(fields=[
        "timestamp", "id", "symptom_abdominal_pain", "symptom_lack_of_appetite_skipping_meals", "questionare_version"
    ])
).process()


logging.info("Great Success!")
