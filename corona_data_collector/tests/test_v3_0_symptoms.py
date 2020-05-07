import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


TEST_FIELDS = {
    # db field                                           corona_bot_answers field
    "version":                                           "questionare_version",
    "symptoms_abdominal_pain":                           "symptoms_abdominal_pain",
    "symptoms_lack_of_appetite_or_skipping_meals":       "symptoms_lack_of_appetite_skipping_meals",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "symptoms_abdominal_pain":                         (None, ""),
        "symptoms_lack_of_appetite_or_skipping_meals":     (None, ""),
    },
    734863: {
        "version": ("3.0.0",),
        "symptoms_abdominal_pain":                         (True, "1"),
        "symptoms_lack_of_appetite_or_skipping_meals":     (True, "1"),
    },
    734839: {
        "version": ("3.0.0",),
        "symptoms_abdominal_pain":                         (None, "0"),
        "symptoms_lack_of_appetite_or_skipping_meals":     (True, "1"),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)
