import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row("4.0.0", limit_rows=99999, db_dump_to_path="data/test_symptoms_duration_db_dump")
# 851523
# 850724
# 849379

MOCK_DATA = {
    849379: {
        "version": "4.2.0",
    },
    850724: {
        "version": "4.2.0",
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
    851523: {
        "symptoms_rash": (None, ""),
        "symptoms_rash_duration": (None, ""),
    },
    850724: {
        "symptoms_rash": (True, "1"),
        "symptoms_rash_duration": ("5",),
    },
    849379: {
        "symptoms_rash": (None, "0"),
        "symptoms_rash_duration": (None, ""),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA, mock_data=MOCK_DATA)
