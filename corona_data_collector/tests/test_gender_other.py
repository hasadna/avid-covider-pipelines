import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row("3.0.0", "sex", "male")  # 734797
# get_db_test_row("3.0.0", "sex", "female")  # 734800
# get_db_test_row("3.0.0", "sex", "other")  # 734839


TEST_FIELDS = {
    # db field            corona_bot_answers field
    "version":            "questionare_version",
    "sex":                "gender"
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "sex": ("female", "1")
    },
    400000: {
        "version": ("2.2.2",),
        "sex": ("male", "0")
    },
    734797: {
        "version": ("3.0.0",),
        "sex": ("male", "0")
    },
    734800: {
        "version": ("3.0.0",),
        "sex": ("female", "1")
    },
    734839: {
        "version": ("3.0.0",),
        "sex": ("other", "2")
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)
