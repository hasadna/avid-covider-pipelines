import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


TEST_FIELDS = {
    # db field                                           corona_bot_answers field
    "version":                                           "questionare_version",
    "routine_uses_public_transportation":                "public_transportation_last_week",
    "routine_uses_public_transportation_bus":            "public_transportation_bus",
    "routine_uses_public_transportation_train":          "public_transportation_train",
    "routine_uses_public_transportation_taxi":           "public_transportation_taxi",
    "routine_uses_public_transportation_other":          "public_transportation_other",
    "routine_visits_prayer_house":                       "habits_prayer_house",
    "routine_wears_mask":                                "last_week_wear_mask",
    "routine_wears_gloves":                              "last_week_wear_gloves",
    "routine_last_asked":                                "routine_last_asked",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    94: {
        "version": ("0.1.0",),
        "routine_uses_public_transportation":       (None, ""),
        "routine_uses_public_transportation_bus":   (None, ""),
        "routine_uses_public_transportation_train": (None, ""),
        "routine_uses_public_transportation_taxi":  (None, ""),
        "routine_uses_public_transportation_other": (None, ""),
        "routine_visits_prayer_house":              (None, ""),
        "routine_wears_mask":                       (None, ""),
        "routine_wears_gloves":                     (None, ""),
        "routine_last_asked":                       (None, ""),
    },
    400000: {
        "version": ("2.2.2",),
        "routine_uses_public_transportation":       (None, ""),
        "routine_uses_public_transportation_bus":   (None, ""),
        "routine_uses_public_transportation_train": (None, ""),
        "routine_uses_public_transportation_taxi":  (None, ""),
        "routine_uses_public_transportation_other": (None, ""),
        "routine_visits_prayer_house":              (None, ""),
        "routine_wears_mask":                       (None, ""),
        "routine_wears_gloves":                     (None, ""),
        "routine_last_asked":                       (None, ""),
    },
    # get_db_test_row("3.0.0", "routine_uses_public_transportation", "false", show_fields=[
    #     "routine_uses_public_transportation",  "routine_uses_public_transportation_bus", "routine_uses_public_transportation_train", "routine_uses_public_transportation_taxi", "routine_uses_public_transportation_other"
    # ])  # 734885
    734885: {
        "version": ("3.0.0",),
        "routine_uses_public_transportation":       (False, "0"),
        "routine_uses_public_transportation_bus":   (None,  "0"),
        "routine_uses_public_transportation_train": (None,  "0"),
        "routine_uses_public_transportation_taxi":  (None,  "0"),
        "routine_uses_public_transportation_other": (None,  "0"),
        "routine_visits_prayer_house":              (False, "0"),
        "routine_wears_mask":                       ("always", "3"),
        "routine_wears_gloves":                     ("never", "0"),
        "routine_last_asked":                       (1588837240349, "2020-05-07T07:40:40.000Z"),
    },
    # get_db_test_row("3.0.0", "routine_uses_public_transportation", "true", show_fields=[
    #     "routine_uses_public_transportation",  "routine_uses_public_transportation_bus", "routine_uses_public_transportation_train", "routine_uses_public_transportation_taxi", "routine_uses_public_transportation_other"
    # ])  # 732749 (taxi=null, train=null, bus=true, other=null)
    732749: {
        "version": ("3.0.0",),
        "routine_uses_public_transportation":       (True, "1"),
        "routine_uses_public_transportation_bus":   (True, "1"),
        "routine_uses_public_transportation_train": (None, "0"),
        "routine_uses_public_transportation_taxi":  (None, "0"),
        "routine_uses_public_transportation_other": (None, "0"),
        "routine_visits_prayer_house":              (False, "0"),
        "routine_wears_mask":                       ("always", "3"),
        "routine_wears_gloves":                     ("never", "0"),
        "routine_last_asked":                       (1588795349500, "2020-05-06T20:02:29.000Z"),
    },
     # 732802 (taxi=true, train=null, bus=true, other=null)
    732802: {
        "version": ("3.0.0",),
        "routine_uses_public_transportation":       (True, "1"),
        "routine_uses_public_transportation_bus":   (True, "1"),
        "routine_uses_public_transportation_train": (None, "0"),
        "routine_uses_public_transportation_taxi":  (True, "1"),
        "routine_uses_public_transportation_other": (None, "0"),
        "routine_visits_prayer_house":              (False, "0"),
        "routine_wears_mask":                       ("mostly_yes", "2"),
        "routine_wears_gloves":                     ("mostly_no", "1"),
        "routine_last_asked":                       (1588795887517, "2020-05-06T20:11:27.000Z"),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA)
