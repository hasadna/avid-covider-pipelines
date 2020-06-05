import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row("3.0.0", "age", "8")  # 732696
# get_db_test_row("3.0.0", "age", "14")  # 732700
# get_db_test_row("3.0.0", "age", "22")  # 734356

MOCK_DATA = {
    # id in DB:
    #   "db field": "mock value"
    732696: {
        "version": "3.1.0",
        "school_name": "בית ספר יסודי נווה חמציצים",
    },
    732700: {
        "version": "3.1.1",
        "school_name": "חטיבת ביניים נווה חמציצים",
    },
    734356: {
        "version": "3.1.2",
        "school_name": "",
    },
}
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
    732696: {
        "version": ("3.1.0",),
        "school_name": ("בית ספר יסודי נווה חמציצים", "בית ספר יסודי נווה חמציצים")
    },
    732700: {
        "version": ("3.1.1",),
        "school_name": ("חטיבת ביניים נווה חמציצים", "חטיבת ביניים נווה חמציצים")
    },
    734356: {
        "version": ("3.1.2",),
        "school_name": ("", "")
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA, mock_data=MOCK_DATA)
