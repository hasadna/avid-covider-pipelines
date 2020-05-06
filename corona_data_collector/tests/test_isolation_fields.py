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
        "__id", "__created", "main_age", "insulation_start_date", "exposure_status", "insulation_reason"
    ])
).process()


Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_29_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: (str(row["isolation_start_date"]), row["isolation"]),
        {
            "676579": ["corona_bot_answers_29_4_2020_with_coords", "0020-03-19", "6"],
            "676580": ["corona_bot_answers_29_4_2020_with_coords", "0020-03-19", "6"],
            "180074": ["corona_bot_answers_25_3_2020_with_coords", "2020-03-18", "2"],
            "180075": ["corona_bot_answers_25_3_2020_with_coords", "0", "0"],
        }
    ),
    printer(fields=[
        "age", "timestamp", "id", "insulation_exposure_date", "insulation_patient_number", "isolation_reason",
        "isolation_returned_from_abroad_date", "isolation_start_date", "isolation",
    ])
).process()


logging.info("Great Success!")
