import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


def _mock_habits_fields(id, created, data):
    if id == 600304:
        logging.info("Mocking version 2.8 for id 600304 with public_transportation_last_week = false , routine_visits_prayer_house = true")
        data["public_transportation_last_week"] = False
        data["routine_visits_prayer_house"] = True
        data["version"] = "2.8.0"
    elif id == 676580:
        logging.info("Mocking version 2.8 for id 676580 with public_transportation_last_week = true , public_transportation_bus = true, routine_visits_prayer_house = false")
        data["public_transportation_last_week"] = True
        data["public_transportation_bus"] = True
        data["routine_visits_prayer_house"] = False
        data["version"] = "2.8.0"
    elif id == 676581:
        logging.info("Mocking version 2.8 for id 676581 with routine_visits_prayer_house = no_response")
        data["routine_visits_prayer_house"] = "no_response"
        data["version"] = "2.8.0"
    return id, created, data


Flow(
    load_from_db.flow({
        "where": "id in (94, 180075, 600304, 600895, 676580, 676581, 701508)",
        "filter_db_row_callback": _mock_habits_fields
    }),
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "version",
        "public_transportation_last_week", "public_transportation_bus", "public_transportation_train", "public_transportation_taxi", "public_transportation_other",
        "routine_visits_prayer_house",
    ]),
).process()
Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_22_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_20_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_29_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_2_5_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: [str(row[k]) for k in [
            "questionare_version", "public_transportation_last_week", "public_transportation_bus", "public_transportation_train", "public_transportation_taxi", "public_transportation_other",
            "habits_prayer_house"
        ]],
        {
            #                                                               last weeek      bus      train      taxi      other     prayer_house
            "94": ["corona_bot_answers_22_3_2020_with_coords",     "0.1.0", "",             "",      "",        "",       "",       ""],
            "180075": ["corona_bot_answers_25_3_2020_with_coords", "1.0.1", "",             "",      "",        "",       "",       ""],
            "600304": ["corona_bot_answers_20_4_2020_with_coords", "2.8.0", "0",            "0",     "0",       "0",      "0",      "1"],
            "600895": ["corona_bot_answers_20_4_2020_with_coords", "2.6.0", "",             "",      "",        "",       "",       ""],
            "676580": ["corona_bot_answers_29_4_2020_with_coords", "2.8.0", "1",            "1",     "0",       "0",      "0",      "0"],
            "676581": ["corona_bot_answers_29_4_2020_with_coords", "2.8.0", "0",            "0",     "0",       "0",      "0",      "2"],
            "701508": ["corona_bot_answers_2_5_2020_with_coords",  "2.7.6", "",             "",      "",        "",       "",       ""],
        }
    ),
    printer(fields=[
        "timestamp", "id", "questionare_version",
        "public_transportation_last_week", "public_transportation_bus", "public_transportation_train", "public_transportation_taxi", "public_transportation_other",
        "habits_prayer_house"
    ])
).process()


logging.info("Great Success!")
