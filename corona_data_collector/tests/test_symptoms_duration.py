import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)


# get_db_test_row("4.0.0", limit_rows=99999, db_dump_to_path="data/test_symptoms_duration_db_dump")
# 851523
# 850724
# 849379

MOCK_DATA = {
    851523: {"version": "4.1.0"},
    850724: {"version": "4.1.0"},
    849379: {"version": "4.1.0"},
}
TEST_FIELDS = {
    # db field                                              corona_bot_answers field
    "symptoms_clogged_nose":                                "symptoms_clogged_nose",
    "symptoms_clogged_nose_duration":                       "symptoms_clogged_nose_duration",
    "symptoms_lack_of_appetite_or_skipping_meals":          "symptoms_lack_of_appetite_skipping_meals",
    "symptoms_lack_of_appetite_or_skipping_meals_duration": "symptoms_lack_of_appetite_skipping_meals_duration",
    "symptoms_nausea_and_vomiting":                         "symptoms_nausea_and_vomiting",
    "symptoms_nausea_and_vomiting_duration":                "symptoms_nausea_and_vomiting_duration",
    "symptoms_breath_shortness":                            "symptoms_breath_shortness",
    "symptoms_breath_shortness_duration":                   "symptoms_breath_shortness_duration",
    "symptoms_muscles_pain":                                "symptoms_muscles_pain",
    "symptoms_muscles_pain_duration":                       "symptoms_muscles_pain_duration",
    "symptoms_tiredness_or_fatigue":                        "symptoms_tiredness_or_fatigue",
    "symptoms_tiredness_or_fatigue_duration":               "symptoms_tiredness_or_fatigue_duration",
    "symptoms_headache":                                    "symptoms_headache",
    "symptoms_headache_duration":                           "symptoms_headache_duration",
    "symptoms_smell_taste_loss":                            "symptoms_smell_taste_loss",
    "symptoms_smell_taste_loss_duration":                   "symptoms_smell_taste_loss_duration",
    "symptoms_confusion":                                   "symptoms_confusion",
    "symptoms_confusion_duration":                          "symptoms_confusion_duration",
    "symptoms_moist_cough":                                 "symptoms_moist_cough",
    "symptoms_moist_cough_duration":                        "symptoms_moist_cough_duration",
    "symptoms_dry_cough":                                   "symptoms_dry_cough",
    "symptoms_dry_cough_duration":                          "symptoms_dry_cough_duration",
    "symptoms_diarrhea":                                    "symptoms_diarrhea",
    "symptoms_diarrhea_duration":                           "symptoms_diarrhea_duration",
    "symptoms_abdominal_pain":                              "symptoms_abdominal_pain",
    "symptoms_abdominal_pain_duration":                     "symptoms_abdominal_pain_duration",
    "symptoms_sore_throat":                                 "symptoms_sore_throat",
    "symptoms_sore_throat_duration":                        "symptoms_sore_throat_duration",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    851523: {
        "symptoms_nausea_and_vomiting": (True, "1"),
        "symptoms_nausea_and_vomiting_duration": ("0", "0"),
        "symptoms_moist_cough": (True, "1"),
        "symptoms_moist_cough_duration": ("4", "4"),
    },
    850724: {
        "symptoms_clogged_nose": (True, "1"),
        "symptoms_clogged_nose_duration": ("5", "5"),
        "symptoms_lack_of_appetite_or_skipping_meals": (True, "1"),
        "symptoms_lack_of_appetite_or_skipping_meals_duration": ("5", "5"),
        "symptoms_nausea_and_vomiting": (True, "1"),
        "symptoms_nausea_and_vomiting_duration": ("1", "1"),
        "symptoms_breath_shortness": (True, "1"),
        "symptoms_breath_shortness_duration": ("2",),
        "symptoms_muscles_pain": (True, "1"),
        "symptoms_muscles_pain_duration": ("5",),
        "symptoms_tiredness_or_fatigue": (True, "1"),
        "symptoms_tiredness_or_fatigue_duration": ("5",),
        "symptoms_headache": (True, "1"),
        "symptoms_headache_duration": ("5",),
        "symptoms_smell_taste_loss": (True, "1"),
        "symptoms_smell_taste_loss_duration": ("5",),
        "symptoms_dry_cough": (True, "1"),
        "symptoms_dry_cough_duration": ("10",),
        "symptoms_abdominal_pain": (True, "1"),
        "symptoms_abdominal_pain_duration": ("2",),
        "symptoms_sore_throat": (True, "1"),
        "symptoms_sore_throat_duration": ("5",),
    },
    849379: {},
}


for db_field, _ in TEST_FIELDS.items():
    for _, values in TEST_DATA.items():
        if db_field not in values:
            values[db_field] = (None, "0")


run_full_db_data_test(TEST_FIELDS, TEST_DATA, mock_data=MOCK_DATA)
