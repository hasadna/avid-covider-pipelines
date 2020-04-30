import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


Flow(
    load_from_db.flow({
        "where": "id in (1199, 6406, 686719, 672579, 650000)",
    }),
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "main_age", "diagnosed_location", "hospitalized"
    ])
).process()


Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_22_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_26_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_28_4_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_30_4_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: [str(row["diagnosed_location"])],
        {
            "1199": ['corona_bot_answers_22_3_2020_with_coords', '1'],
            "6406": ['corona_bot_answers_22_3_2020_with_coords', '3'],
            "650000": ['corona_bot_answers_26_4_2020_with_coords', '0'],
            "672579": ['corona_bot_answers_28_4_2020_with_coords', '1'],
            "686719": ['corona_bot_answers_30_4_2020_with_coords', '2'],
        }
    ),
    printer(fields=[
        "age", "timestamp", "id", "diagnosed_location"
    ])
).process()


logging.info("Great Success!")
