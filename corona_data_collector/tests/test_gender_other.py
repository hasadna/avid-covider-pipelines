import random
import logging
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec
from dataflows import printer, Flow, load
from .common import test_corona_bot_answers


logging.basicConfig(level=logging.INFO)


def _mock_gender_other(rows):
    for row in rows:
        if row["__id"] in [460001, 460002]:
            logging.info("Mocking sex for id %s (%s -> %s)" % (row["__id"], row["sex"], '"other"'))
            row["sex"] = '"other"'
        yield row


Flow(
    load_from_db.flow({
        "where": "id > 460000",
        "limit_rows": 5
    }),
    _mock_gender_other,
    add_gps_coordinates.flow({
        "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
        "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
    }),
    export_corona_bot_answers.flow({
        "destination_output": "data/corona_data_collector/destination_output"
    }),
    printer(fields=[
        "__id", "__created", "main_age", "sex"
    ])
).process()
Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_10_4_2020_with_coords.csv"),
    test_corona_bot_answers(
        lambda row: (str(row["gender"]),),
        {
            "460001": ["corona_bot_answers_10_4_2020_with_coords", "2"],
            "460002": ["corona_bot_answers_10_4_2020_with_coords", "2"],
            "460003": ["corona_bot_answers_10_4_2020_with_coords", "1"],
            "460004": ["corona_bot_answers_10_4_2020_with_coords", "0"],
            "460005": ["corona_bot_answers_10_4_2020_with_coords", "1"],
            "460006": ["corona_bot_answers_10_4_2020_with_coords", "1"],
        }
    ),
    printer(fields=[
        "age", "timestamp", "id", "gender"
    ])
).process()


logging.info("Great Success!")
