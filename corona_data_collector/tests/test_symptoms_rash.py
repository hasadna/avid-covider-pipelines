import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row("4.2.0", limit_rows=99999, db_dump_to_path="data/test_symptoms_rash_db_dump")
# 910027
# 910028
# 910029

MOCK_DATA = {
    910028: {
        "symptoms_rash": True,
        "symptoms_rash_duration": "5",
    }
}
TEST_FIELDS = {
    # db field                corona_bot_answers field
    "symptoms_rash":          "symptoms_rash",
    "symptoms_rash_duration": "symptoms_rash_duration",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    400000: {
        "symptoms_rash": (None, ""),
        "symptoms_rash_duration": (None, ""),
    },
    910027: {
        "symptoms_rash": (None, "0"),
        "symptoms_rash_duration": (None, ""),
    },
    910028: {
        "symptoms_rash": (True, "1"),
        "symptoms_rash_duration": ("5",),
    },
    910029: {
        "symptoms_rash": (None, "0"),
        "symptoms_rash_duration": (None, ""),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA, mock_data=MOCK_DATA)
