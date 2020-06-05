import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row(where="data->>'school_name' != ''")  # 863690
# get_db_test_row(where="data->>'school_name' != ''")  # 863783
# get_db_test_row("3.1.0", show_fields=["school_name"])  # 863705

TEST_FIELDS = {
    # db field            corona_bot_answers field
    "version":            "questionare_version",
    "school_name":        "school_name"
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "school_name": (None, "")
    },
    400000: {
        "version": ("2.2.2",),
        "school_name": (None, "")
    },
    863690: {
        "version": ("3.1.0",),
        "school_name": ("רב תחומי חדרה ", "רב תחומי חדרה")
    },
    863783: {
        "version": ("3.1.0",),
        "school_name": ("רבין ", "רבין")
    },
    863705: {
        "version": ("3.1.0",),
        "school_name": (None, "")
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)
