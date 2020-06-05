import logging
from dataflows import Flow, load, printer
from corona_data_collector import load_from_db, add_gps_coordinates, export_corona_bot_answers
import random
from avid_covider_pipelines.utils import get_parameters_from_pipeline_spec


def test_corona_bot_answers(actual_row_callback, expected_data, extra_row_test_callback=None):

    def _test(rows):
        logging.info("Testing corona_bot_answers...")
        for row in rows:
            yield row
            expected_row = expected_data[row["id"]]
            actual_row = [rows.res.name, *actual_row_callback(row)]
            assert expected_row == actual_row, "%s: %s" % (row["id"], actual_row[1:])
            if extra_row_test_callback:
                extra_row_test_callback(row)
        logging.info("Testing completed successfully")

    return _test


def run_full_db_data_test(test_fields, test_data, dry_run=False, get_real_coords=False, mock_data=None):
    _test_data = {id: {} for id in test_data}

    def _db_row_callback(id, created, data):
        if id in test_data:
            if mock_data and id in mock_data:
                for mock_field, mock_value in mock_data[id].items():
                    data[mock_field] = mock_value
            for db_field, assert_values in test_data[id].items():
                if data.get(db_field) != assert_values[0]:
                    msg = "Invalid data in db field %s id %s. expected=%s actual=%s" % (db_field, id, assert_values[0], data.get(db_field))
                    if dry_run:
                        logging.info(msg)
                    else:
                        raise AssertionError(msg)
            logging.info("DB data is valid for id %s (validated %s fields)" % (id, len(test_data[id])))
            _test_data[id]["created"] = created
        return id, created, data

    Flow(
        load_from_db.flow({
            "where": "id in (%s)" % ", ".join(map(str, test_data.keys())),
            "filter_db_row_callback": _db_row_callback,
        }),
        add_gps_coordinates.flow({
            "source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["source_fields"],
            "workplace_source_fields": get_parameters_from_pipeline_spec("pipeline-spec.yaml", "corona_data_collector", "corona_data_collector.add_gps_coordinates")["workplace_source_fields"],
            **({
                "get-coords-callback": lambda street, city: (random.uniform(29, 34), random.uniform(34, 36), int(street != city))
            } if get_real_coords == False else {})
        }),
        export_corona_bot_answers.flow({
            "destination_output": "data/corona_data_collector/destination_output"
        }),
        printer(fields=[
            "__id", "__created", *test_fields.keys()
        ]),
    ).process()

    _test_assertions = {
        str(id): [
            "corona_bot_answers_%s_with_coords" % _test_data[id]["created"].strftime("%-d_%-m_%Y"),
        ]
        for id in test_data
    }

    for db_field in sorted(test_fields.keys()):
        for id, assertions in _test_assertions.items():
            if len(test_data[int(id)][db_field]) == 1:
                assertions.append(test_data[int(id)][db_field][0])
            else:
                assertions.append(test_data[int(id)][db_field][1])

    def _test_corona_bot_answers_get_row(row):
        return (
            str(row[test_fields[db_field]])
            for db_field in sorted(test_fields.keys())
        )

    try:
        Flow(
            *[
                load(
                    "data/corona_data_collector/destination_output/corona_bot_answers_%s_with_coords.csv" % created.strftime(
                        "%-d_%-m_%Y"))
                for created in set([data["created"] for data in _test_data.values()])
            ],
            test_corona_bot_answers(
                _test_corona_bot_answers_get_row,
                _test_assertions
            ),
            printer(fields=[
                "timestamp", "id", *test_fields.values()
            ])
        ).process()
    except AssertionError as e:
        raise AssertionError(
            str(e) + " fields: " + str([test_fields[db_field] for db_field in sorted(test_fields.keys())]))

    logging.info("Great Success!")


def get_db_test_row(version=None, field_name=None, value=None, where=None, show_fields=None):
    if not where:
        where = []
        if version:
            where.append("data->>'version' = '%s'" % version)
        if field_name and value:
            where.append("data->>'%s' = '%s'" % (field_name, value))
        where = " and ".join(where)
    if not show_fields:
        if field_name:
            show_fields = [field_name]
        else:
            show_fields = []
    Flow(
        load_from_db.flow({
            "where": where,
            "limit_rows": 10,
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
            "__id", "__created", *show_fields
        ]),
    ).process()
