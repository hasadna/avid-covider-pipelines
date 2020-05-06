import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


def _mock_work_outside(id, created, data):
    if id == 600304:
        logging.info("Mocking version 3.0 for id 600304 with work_outside = no")
        data["work_outside"] = False
        data["version"] = "3.0.0"
    elif id == 676580:
        logging.info("Mocking version 3.0 for id 676580 with work_outside = yes + full details")
        data["work_outside"] = True
        data["routine_workplace_single_location"] = True
        data["work_outside_avg_weekly_hours"] = 3
        data["work_outside_city_town"] = "תל אביב"
        data["work_outside_street"] = "הרצל"
        data["version"] = "3.0.0"
    elif id == 676581:
        logging.info("Mocking version 3.0 for id 676581 with work_outside = yes + minimal details")
        data["work_outside"] = True
        data["routine_workplace_single_location"] = False
        data["work_outside_avg_weekly_hours"] = 55
        data["version"] = "3.0.0"
    elif id == 180075:
        logging.info("Mocking version 3.0 for id 180075 with work_outside = yes + invalid details")
        data["work_outside"] = True
        data["work_outside_avg_weekly_hours"] = "foobar"
        data["work_outside_city_town"] = 55
        data["work_outside_street"] = 44
        data["version"] = "3.0.0"
    return id, created, data


Flow(
    load_from_db.flow({
        "where": "id in (94, 180075, 600304, 600895, 676580, 676581, 701508)",
        "filter_db_row_callback": _mock_work_outside
    }),
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "workplace_source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["workplace_source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "version", "work_outside", "work_outside_avg_weekly_hours", "work_outside_city_town",
        "work_outside_street", "workplace_lat", "workplace_lng", "workplace_street_accurate", "routine_workplace_single_location"
    ]),
).process()


def _test_workplace_lat_lng(row):
    if row["id"] in ["180075", "676580"]:
        assert row["workplace_lat"] and row["workplace_lng"], (row["id"], row["workplace_lat"], row["workplace_lng"])
    else:
        assert row["workplace_lat"] == "0" and row["workplace_lng"] == "0", (row["id"], row["workplace_lat"], row["workplace_lng"])


Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_22_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_20_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_29_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_2_5_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: (
            str(row["questionare_version"]), str(row["work_outside"]), str(row["work_outside_avg_weekly_hours"]),
            str(row["workplace_city_town"]), str(row["workplace_street"]),
            str(row["workplace_street_accurate"]),
            str(row["workplace_single_location"])
        ),
        {
                                                                       #  work_outside    hours    city_town    street    accurate    single_location
            "94": ["corona_bot_answers_22_3_2020_with_coords", "0.1.0",       "",         "",      "",          "",       "0",        ""],
            "180075": ["corona_bot_answers_25_3_2020_with_coords", "3.0.0",   "1",        "0",     "55",        "44",     "1",        "0"],
            "600304": ["corona_bot_answers_20_4_2020_with_coords", "3.0.0",   "0",        "0",     "",          "",       "0",        "0"],
            "600895": ["corona_bot_answers_20_4_2020_with_coords", "2.6.0",   "",         "",      "",          "",       "0",        ""],
            "676580": ["corona_bot_answers_29_4_2020_with_coords", "3.0.0",   "1",        "3",     "תל אביב",   "הרצל",   "1",        "1"],
            "676581": ["corona_bot_answers_29_4_2020_with_coords", "3.0.0",   "1",        "55",    "",          "",       "0",        "0"],
            "701508": ["corona_bot_answers_2_5_2020_with_coords", "2.7.6",    "",          "",     "",          "",       "0",        ""],
        },
        _test_workplace_lat_lng
    ),
    printer(fields=[
        "timestamp", "id", "questionare_version", "work_outside", "work_outside_avg_weekly_hours",
        "workplace_city_town", "workplace_street", "workplace_lat", "workplace_lng", "workplace_street_accurate",
        "workplace_single_location"
    ])
).process()


logging.info("Great Success!")
