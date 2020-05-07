import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


TEST_FIELDS = {
    # db field                                           corona_bot_answers field
    "version":                                           "questionare_version",
    "routine_workplace_is_outside":                      "work_outside",
    "routine_workplace_weekly_hours":                    "work_outside_avg_weekly_hours",
    "routine_workplace_city_town":                       "workplace_city_town",
    "routine_workplace_street":                          "workplace_street",
    "routine_workplace_single_location":                 "workplace_single_location",
    "workplace_lat":                                     "workplace_lat",
    "workplace_lng":                                     "workplace_lng",
    "workplace_street_accurate":                         "workplace_single_location",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "routine_workplace_is_outside":       (None, ""),
        "routine_workplace_weekly_hours":     (None, ""),
        "routine_workplace_city_town":        (None, ""),
        "routine_workplace_street":           (None, ""),
        "routine_workplace_single_location":  (None, ""),
        "workplace_lat":                      (None, "0"),
        "workplace_lng":                      (None, "0"),
        "workplace_street_accurate":          (None, ""),
    },
    734863: {
        "version": ("3.0.0",),
        "routine_workplace_is_outside":       (False, "0"),
        "routine_workplace_weekly_hours":     (None, "0"),
        "routine_workplace_city_town":        (None, ""),
        "routine_workplace_street":           (None, ""),
        "routine_workplace_single_location":  (None, "0"),
        "workplace_lat":                      (None, "0"),
        "workplace_lng":                      (None, "0"),
        "workplace_street_accurate":          (None, "0"),
    },
    734839: {
        "version": ("3.0.0",),
        "routine_workplace_is_outside":       (True, "1"),
        "routine_workplace_weekly_hours":     ("5", "5"),
        "routine_workplace_city_town":        ("מזכרת בתיה", "מזכרת בתיה"),
        "routine_workplace_street":           ( "הגפן", "הגפן"),
        "routine_workplace_single_location":  (True, "1"),
        "workplace_lat":                      (None, "31.8561771"),
        "workplace_lng":                      (None, "34.8366475"),
        "workplace_street_accurate":          (None, "1"),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA, get_real_coords=True)