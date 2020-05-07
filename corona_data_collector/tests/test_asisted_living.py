import logging
from corona_data_collector.tests.common import run_full_db_data_test, get_db_test_row


logging.basicConfig(level=logging.INFO)


TEST_FIELDS = {
    # db field            corona_bot_answers field
    "version":            "questionare_version",
    "is_assisted_living": "assisted_living"
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "is_assisted_living": (None, "")
    },
    400000: {
        "version": ("2.2.2",),
        "is_assisted_living": (None, "")
    },
    732704: {
        "version": ("3.0.0",),
        "is_assisted_living": (None, "")
    },
    732705: {
        "version": ("3.0.0",),
        "is_assisted_living": (False, "0")
    },
    733451: {
        "version": ("3.0.0",),
        "is_assisted_living": ("no_response", "2")
    },
    733631: {
        "version": ("3.0.0",),
        "is_assisted_living": (True, "1")
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)