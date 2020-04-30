import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


Flow(
    load_from_db.flow({
        "where": "id in (180074, 180075, 676579, 676580)"
    }),
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "main_age", "medical_staff_member", "engagement_source", "alias", "layout"
    ])
).process()

Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_29_4_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: (str(row["medical_staff_member"]), str(row["engagement_source"]), str(row["layout"])),
        {
            "180074": ["corona_bot_answers_25_3_2020_with_coords", "", "", ""],
            "180075": ["corona_bot_answers_25_3_2020_with_coords", "", "", ""],
            "676579": ["corona_bot_answers_29_4_2020_with_coords", "0", "direct", "desktop"],
            "676580": ["corona_bot_answers_29_4_2020_with_coords", "0", "direct", "desktop"],
        }
    ),
    printer(fields=[
        "age", "timestamp", "id", "medical_staff_member", "engagement_source", "alias", "layout"
    ])
).process()


logging.info("Great Success!")
