import logging
from dataflows import Flow, printer, load
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
import random
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec


logging.basicConfig(level=logging.INFO)

# get_db_test_row(
#     where="data->>'exposure_status' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms', 'has-symptoms', 'contact-with-patient', 'back-from-abroad', 'voluntary', 'not-insulated')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_status' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms', 'has-symptoms', 'contact-with-patient', 'back-from-abroad', 'voluntary', 'not-insulated')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_status' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms', 'has-symptoms', 'contact-with-patient', 'back-from-abroad')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_status' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_status' in ('insulation_with_family', 'diagnosed', 'has_symptoms')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_status' in ('diagnosed', 'has_symptoms')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_reason' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms', 'has-symptoms', 'contact-with-patient', 'back-from-abroad', 'voluntary', 'not-insulated')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_reason' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has_symptoms', 'has-symptoms', 'not-insulated')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# get_db_test_row(
#     where="data->>'insulation_reason' in ('insulation_with_family', 'diagnosed', 'hospitalized', 'has-symptoms', 'not-insulated')",
#     show_fields=["exposure_status", "insulation_status", "insulation_reason"]
# )
# exit(42)

source_test_data = [
    {
        "exposure_status": "diagnosed",
        "insulation_status": "null",
        "insulation_reason": "null",
        "ids": [170690, 170741, 175709],
        "expected": {
            "isolation_reason": "0",
            "isolation": "5",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "insulation_with_family",
        "insulation_status": "null",
        "insulation_reason": "null",
        "ids": [181882, 181883, 181889],
        "expected": {
            "isolation_reason": "0",
            "isolation": "6",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "not-insulated",
        "insulation_reason": "null",
        "ids": [86, 87, 88],
        "expected": {
            "isolation_reason": "",
            "isolation": "0",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "voluntary",
        "insulation_reason": "null",
        "ids": [90],
        "expected": {
            "isolation_reason": "",
            "isolation": "1",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "contact-with-patient",
        "insulation_reason": "null",
        "ids": [110, 134, 153],
        "expected": {
            "isolation_reason": "",
            "isolation": "3",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "back-from-abroad",
        "insulation_reason": "null",
        "ids": [165, 176, 177],
        "expected": {
            "isolation_reason": "",
            "isolation": "2",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "has-symptoms",
        "insulation_reason": "null",
        "ids": [292],
        "expected": {
            "isolation_reason": "",
            "isolation": "4",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "hospitalized",
        "insulation_reason": "null",
        "ids": [1199, 9828, 13263],
        "expected": {
            "isolation_reason": "",
            "isolation": "5",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "insulation",
        "insulation_status": "null",
        "insulation_reason": "back-from-abroad",
        "ids": [169603],
        "expected": {
            "isolation_reason": "0",
            "isolation": "2",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "insulation",
        "insulation_status": "null",
        "insulation_reason": "voluntary",
        "ids": [169630],
        "expected": {
            "isolation_reason": "0",
            "isolation": "1",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "insulation",
        "insulation_status": "null",
        "insulation_reason": "contact-with-patient",
        "ids": [169637],
        "expected": {
            "isolation_reason": "0",
            "isolation": "3",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },
    {
        "exposure_status": "insulation",
        "insulation_status": "null",
        "insulation_reason": "has_symptoms",
        "ids": [169728, 177296],
        "expected": {
            "isolation_reason": "0",
            "isolation": "4",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None
        }
    },

    # v4 data
    {
        "exposure_status": "null",
        "insulation_status": "insulation_with_family",
        "insulation_reason": "voluntary",
        "ids": [849384, 849559, 849908],
        "expected": {
            "isolation_reason": "0",
            "isolation": "0",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None,
            "v4_insulation_status": "2",
            "v4_insulation_reason": "4",
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "insulation_with_family",
        "insulation_reason": "contact_with_patient",
        "ids": [849448],
        "expected": {
            "isolation_reason": "0",
            "isolation": "0",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None,
            "v4_insulation_status": "2",
            "v4_insulation_reason": "2",
        }
    },
    {
        "exposure_status": "null",
        "insulation_status": "insulation_with_family",
        "insulation_reason": "has_symptoms",
        "ids": [849596, 849599, 849878],
        "expected": {
            "isolation_reason": "0",
            "isolation": "0",
            "exposure_status": None,
            "insulation_status": None,
            "insulation_reason": None,
            "isolation_status": None,
            "v4_insulation_status": "2",
            "v4_insulation_reason": "3",
        }
    },
]
test_data = {}
for item in source_test_data:
    ids = item.pop("ids")
    for id in ids:
        test_data[id] = item


def _db_row_callback(id, created, data):
    item = test_data[id]
    for k, v in item.items():
        if k == 'expected': continue
        if v == "null":
            v = None
        val = data.get(k)
        assert v == val, "Invalid data in DB: %s %s (expected '%s' != actual '%s')" % (id, k, v, val)
    return id, created, data


Flow(
    load_from_db.flow({
        "where": "id in (%s)" % ", ".join(map(str, test_data.keys())),
        "filter_db_row_callback": _db_row_callback,
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
        "__id", "__created", "exposure_status", "insulation_status", "insulation_reason"
    ]),
).process()


def _test_corona_bot_answers(row):
    for k, v in test_data[int(row["id"])]["expected"].items():
        assert row.get(k) == v, "%s %s (expected '%s' != actual '%s')" % (row["id"], k, v, row.get(k))


Flow(
    load("data/corona_data_collector/destination_output/corona_bot_answers_22_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_25_3_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_1_6_2020_with_coords.csv"),
    load("data/corona_data_collector/destination_output/corona_bot_answers_2_6_2020_with_coords.csv"),
    _test_corona_bot_answers,
    printer(fields=[
        "timestamp", "id", "exposure_status", "insulation_status", "insulation_reason", "isolation", "isolation_reason", "isolation_status"
    ])
).process()

logging.info("Great Success!")
