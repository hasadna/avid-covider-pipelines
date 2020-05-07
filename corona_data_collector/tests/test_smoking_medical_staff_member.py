import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


TEST_FIELDS = {
    # db field                                           corona_bot_answers field
    "version":                                           "questionare_version",
    "precondition_smoking":                              "smoking",
    "medical_staff_member":                              "medical_staff_member",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "precondition_smoking":     (None, "0"),
        "medical_staff_member":     (None, ""),
    },
    # get_db_test_row("3.0.0", "precondition_smoking", "long_past_smoker")  # 734978
    734978: {
        "version": ("3.0.0",),
        "precondition_smoking":     ("long_past_smoker", "1"),
        "medical_staff_member":     (None, "0"),
    },
    # get_db_test_row("3.0.0", "medical_staff_member", "true")
    #   734839   (true)
    734839: {
        "version": ("3.0.0",),
        "precondition_smoking":     ("daily_smoker", "3"),
        "medical_staff_member":     (True, "1"),
    },
    #   735051   ("true")
    735051: {
        "version": ("3.0.0",),
        "precondition_smoking":     ("never", "0"),
        "medical_staff_member":     ("true", "1"),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)