import logging
from corona_data_collector.tests.common import get_db_test_row, run_full_db_data_test


logging.basicConfig(level=logging.INFO)

# get_db_test_row("4.0.0", limit_rows=9999, db_dump_to_path="data/test_v4_0_0_db_dump")

# covid_positive=true  covid_last_positive_results_date!=null         covid_last_negative_results_date=null  hospitalization_status=false
# hospitalization_start_date=null          hospitalization_end_date=null          hospitalization_icu_required=null
#    850724  851523

# covid_positive=true  covid_last_positive_results_date="2020-05-24"  covid_last_negative_results_date=null  hospitalization_status=false
# hospitalization_start_date="2020-06-01"  hospitalization_end_date="2020-06-03"  hospitalization_icu_required=false
#    854455

# covid_positive=true  covid_last_positive_results_date="2020-05-14"  covid_last_negative_results_date=null  hospitalization_status=true
# hospitalization_start_date="2020-06-28"  hospitalization_end_date=null          hospitalization_icu_required=null
#    854656

# covid_positive=false  covid_last_positive_results_date=null        covid_last_negative_results_date=null   hospitalization_status=null
# hospitalization_start_date=null          hospitalization_end_date=null          hospitalization_icu_required=null
#    849379  849381

# hospitalization_icu_required=true  hospitalization_icu_duration!=null
#    MOCK (using 849381)

MOCK_DATA = {
    849381: {
        "hospitalization_icu_required": True,
        "hospitalization_icu_duration": "5"
    }
}
TEST_FIELDS = {
    # db field                          corona_bot_answers field
    "covid_positive":                   "covid_positive",
    "covid_last_positive_results_date": "covid_last_positive_results_date",
    "covid_last_negative_results_date": "covid_last_negative_results_date",
    "hospitalization_status":           "hospitalization_status",
    "hospitalization_start_date":       "hospitalization_start_date",
    "hospitalization_end_date":         "hospitalization_end_date",
    "hospitalization_icu_required":     "hospitalization_icu_required",
    "hospitalization_icu_duration":     "hospitalization_icu_duration",
    "covid19_check_date":               "covid19_check_date",
    "covid19_check_result":             "covid19_check_result",
}
TEST_DATA = {
    # id in DB:
    #   "db field": ("value in db", "value_in_corona_bot_answers")
    850724: {
        # covid_positive=true  covid_last_positive_results_date!=null         covid_last_negative_results_date=null  hospitalization_status=false
        # hospitalization_start_date=null          hospitalization_end_date=null          hospitalization_icu_required=null
        "covid_positive":                   (True, "1"),
        "covid_last_positive_results_date": ("2020-06-02",),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (False, "0"),
        "hospitalization_start_date":       (None, ""),
        "hospitalization_end_date":         (None, ""),
        "hospitalization_icu_required":     (None, "0"),
        "hospitalization_icu_duration":     (None, ""),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
    851523: {
        # covid_positive=true  covid_last_positive_results_date!=null         covid_last_negative_results_date=null  hospitalization_status=false
        # hospitalization_start_date=null          hospitalization_end_date=null          hospitalization_icu_required=null
        "covid_positive":                   (True, "1"),
        "covid_last_positive_results_date": ("2020-07-10",),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (False, "0"),
        "hospitalization_start_date":       (None, ""),
        "hospitalization_end_date":         (None, ""),
        "hospitalization_icu_required":     (None, "0"),
        "hospitalization_icu_duration":     (None, ""),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
    854455: {
        # covid_positive=true  covid_last_positive_results_date="2020-05-24"  covid_last_negative_results_date=null  hospitalization_status=false
        # hospitalization_start_date="2020-06-01"  hospitalization_end_date="2020-06-03"  hospitalization_icu_required=false
        "covid_positive":                   (True, "1"),
        "covid_last_positive_results_date": ("2020-05-24",),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (False, "0"),
        "hospitalization_start_date":       ("2020-06-01",),
        "hospitalization_end_date":         ("2020-06-03",),
        "hospitalization_icu_required":     (False, "0"),
        "hospitalization_icu_duration":     (None, ""),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
    854656: {
        # covid_positive=true  covid_last_positive_results_date="2020-05-14"  covid_last_negative_results_date=null  hospitalization_status=true
        # hospitalization_start_date="2020-06-28"  hospitalization_end_date=null          hospitalization_icu_required=null
        "covid_positive":                   (True, "1"),
        "covid_last_positive_results_date": ("2020-05-14",),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (True, "1"),
        "hospitalization_start_date":       ("2020-06-28",),
        "hospitalization_end_date":         (None, ""),
        "hospitalization_icu_required":     (None, "0"),
        "hospitalization_icu_duration":     (None, ""),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
    849379: {
        # covid_positive=false  covid_last_positive_results_date=null        covid_last_negative_results_date=null   hospitalization_status=null
        # hospitalization_start_date=null          hospitalization_end_date=null          hospitalization_icu_required=null
        "covid_positive":                   (False, "0"),
        "covid_last_positive_results_date": (None, ""),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (None, "0"),
        "hospitalization_start_date":       (None, ""),
        "hospitalization_end_date":         (None, ""),
        "hospitalization_icu_required":     (None, "0"),
        "hospitalization_icu_duration":     (None, ""),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
    849381: {
        # covid_positive=false  covid_last_positive_results_date=null        covid_last_negative_results_date=null   hospitalization_status=null
        # hospitalization_start_date=null          hospitalization_end_date=null
        # hospitalization_icu_required=true  hospitalization_icu_duration!=null
        "covid_positive":                   (False, "0"),
        "covid_last_positive_results_date": (None, ""),
        "covid_last_negative_results_date": (None, ""),
        "hospitalization_status":           (None, "0"),
        "hospitalization_start_date":       (None, ""),
        "hospitalization_end_date":         (None, ""),
        "hospitalization_icu_required":     (True, "1"),
        "hospitalization_icu_duration":     ("5",),
        "covid19_check_date":               (None, ""),
        "covid19_check_result":             (None, ""),
    },
}


run_full_db_data_test(TEST_FIELDS, TEST_DATA, mock_data=MOCK_DATA)
