import os


class DictObject():

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


try:
    from corona_data_collector import keys
except ImportError:
    import os
    keys = DictObject(
        db_pass=os.environ['CORONA_DATA_COLLECTOR_DB_PASS'],
        gps_url_key=os.environ['CORONA_DATA_COLLECTOR_GPS_URL_KEY'],
    )

gps_url = 'https://maps.googleapis.com/maps/api/geocode/json'
gps_url_key = keys.gps_url_key

db_settings = {
    "host": "35.230.137.198",
    "port": 5432,
    "username": "readonly",
    "password": keys.db_pass,
    "sslrootcert": os.path.join(
        os.environ.get('CORONA_DATA_COLLECTOR_SECRETS_PATH', os.path.dirname(__file__)),
        'certs/server-ca.pem'
    ),
    "sslcert": os.path.join(
        os.environ.get('CORONA_DATA_COLLECTOR_SECRETS_PATH', os.path.dirname(__file__)),
        'certs/client-cert.pem'
    ),
    "sslkey": os.path.join(
        os.environ.get('CORONA_DATA_COLLECTOR_SECRETS_PATH', os.path.dirname(__file__)),
        'certs/client-key.pem'
    ),
}

answer_titles = {
    'id': 'id',
    'created': 'timestamp',
    'age': 'age',
    'sex': 'gender',
    'street': 'street',
    'city_town': 'city',
    'alias': 'alias',
    'chronic_diabetes': 'diabetes',
    'chronic_hypertension': 'hypertension',
    'chronic_ischemic_heart_disease_or_stroke': 'ischemic_heart_disease_or_stroke',
    'chronic_lung_disease': 'lung_disease',
    'chronic_cancer': 'cancer',
    'chronic_kidney_failure': 'kidney_failure',
    'chronic_immune_system_suppression': 'immune_system_suppression',
    'smoking': 'smoking',
    'insulation_status': 'isolation',
    'insulation_start_date': 'isolation_start_date',
    'insulation_reason': 'isolation_reason',
    'insulation_returned_from_abroad_date': 'isolation_returned_from_abroad_date',
    'insulation_patient_number': 'insulation_patient_number',
    'insulation_exposure_date': 'insulation_exposure_date',
    'diagnosed_location': 'diagnosed_location',
    'general_feeling': 'general_feeling',
    'temperature': 'temperature',
    'toplevel_symptoms_cough': 'toplevel_symptoms_cough',
    'toplevel_symptoms_pains': 'toplevel_symptoms_pains',
    'toplevel_symptoms_tiredness': 'toplevel_symptoms_tiredness',
    'toplevel_symptoms_stomach': 'toplevel_symptoms_stomach',
    'symptoms_clogged_nose': 'symptoms_clogged_nose',
    'symptoms_sore_throat': 'symptoms_sore_throat',
    'symptoms_dry_cough': 'symptoms_dry_cough',
    'symptoms_moist_cough': 'symptoms_moist_cough',
    'symptoms_breath_shortness': 'symptoms_breath_shortness',
    'symptoms_muscles_pain': 'symptoms_muscles_pain',
    'symptoms_headache': 'symptoms_headache',
    'symptoms_fatigue': 'symptoms_fatigue',
    'symptoms_infirmity': 'symptoms_infirmity',
    'symptoms_diarrhea': 'symptoms_diarrhea',
    'symptoms_nausea_and_vomiting': 'symptoms_nausea_and_vomiting',
    'symptoms_chills': 'symptoms_chills',
    'symptoms_confusion': 'symptoms_confusion',
    'symptoms_tiredness_or_fatigue': 'symptoms_tiredness_or_fatigue',
    'symptoms_smell_taste_loss': 'symptoms_smell_taste_loss',
    'symptoms_other': 'symptoms_other',
    "abdominal_pain": "symptom_abdominal_pain",
    "lack_of_appetite_or_skipping_meals": "symptom_lack_of_appetite_skipping_meals",
    'exposure_met_people': 'exposure_met_people',
    'flatmates': 'flatmates',
    'flatmates_over_70': 'flatmates_over_70',
    'flatmates_under_18': 'flatmates_under_18',
    'work_serve_over_10': 'work_serve_over_10',
    'met_under_18': 'met_under_18',
    'met_above_18': 'met_above_18',
    'numPreviousReports': 'num_previous_reports',
    'dateFirstReport': 'date_first_report',
    'medical_staff_member': 'medical_staff_member',
    'served_public_last_fortnight': 'helped_10_people_in_last_2_weeks',
    'layout': 'layout',
    'locale': 'locale',
    'version': 'questionare_version',
    'engagementSource': 'engagement_source',
    'notificationsEnabled': 'notifications enabled',
    'covid19_check_date': 'covid19_check_date',
    'covid19_check_result':  'covid19_check_result',
    "main_age": "main_age",
    "main_uid": "main_uid",
    "uid": "uid",
    "num_aliases": "num_aliases",
    "work_outside": "work_outside",
    "work_outside_avg_weekly_hours": "work_outside_avg_weekly_hours",
    "work_outside_city_town": "workplace_city_town",
    "work_outside_street": "workplace_street",
    "assisted_living": "assisted_living",
    "public_transportation_last_week": "public_transportation_last_week",
    "public_transportation_bus": "public_transportation_bus",
    "public_transportation_train": "public_transportation_train",
    "public_transportation_taxi": "public_transportation_taxi",
    "public_transportation_other": "public_transportation_other",
    "routine_visits_prayer_house": "habits_prayer_house",
}
# make sure there aren't any duplicate columns
assert len(set(answer_titles.values())) == len(answer_titles)

keys_to_convert = {
    'precondition_chronic_diabetes': 'chronic_diabetes',
    'precondition_chronic_hypertension': 'chronic_hypertension',
    'precondition_chronic_ischemic_heart_disease_or_stroke': 'chronic_ischemic_heart_disease_or_stroke',
    'precondition_chronic_lung_disease': 'chronic_lung_disease',
    'precondition_chronic_cancer': 'chronic_cancer',
    'precondition_chronic_kidney_failure': 'chronic_kidney_failure',
    'precondition_chronic_immune_system_suppression': 'chronic_immune_system_suppression',
    'hospitalized': 'diagnosed_location',
    'precondition_smoking': 'smoking',
    'symptoms_tiredness_or_fatigue': 'symptoms_fatigue'
}
# make sure there aren't any duplicate columns
assert len(set(keys_to_convert.values())) == len(keys_to_convert)

# both this source fields needs to be converted to "insulation_status" column
# they are handled in the code by checking for null values in the source columns
# ordering is important - the more recent column (insulation_reason) has higher priority - in case both are not null
insulation_status_keys_to_convert = ['insulation_reason', 'exposure_status']

values_to_convert = {
    'sex': {
        'male': 0,
        'female': 1,
        'other': 2
    },
    'smoking': {
        'אף פעם': 0,
        'never': 0,
        'עישנתי בעבר, לפני יותר מחמש שנים': 1,
        'long past smoke': 1,
        'long_past_smokre': 1,
        'עישנתי בעבר, הפסקתי לפני פחות מחמש שנים': 2,
        'עישנתי בעבר, בחמש השנים האחרונות': 2,
        'short_past_smoker': 2,
        'עישון יומיומי': 3,
        'daily_smoker': 3,
    },
    'insulation_status': {
        'not-insulated': 0,
        'none': 0,
        'voluntary': 1,
        'back-from-abroad': 2,
        'contact-with-patient': 3,
        'has-symptoms': 4,
        'has_symptoms': 4,
        'hospitalized': 5,
        'diagnosed': 5,
        'insulation_with_family': 6,
    },
    'diagnosed_location': {
        'none': 0,
        'home': 1,
        'hotel': 2,
        'hospital_from_corona_lab': 3,
        'hospital': 4,
        'recovered': 5,
    },
    'general_feeling': {
        'feel_good': 0,
        'feel_bad': 1,
    },
    'medical_staff_member': {
        'false': 0,
        'true': 1
    },
    "assisted_living": {
        'false': 0,
        'true': 1,
        'no_response': 2
    },
    "routine_visits_prayer_house": {
        "false": 0,
        "true": 1,
        "no_response": 2
    }
}
for field, values in values_to_convert.items():
    values_to_convert[field] = {
        k.lower(): v for k, v in values.items()
    }


values_force_integer = [
    "work_outside_avg_weekly_hours",
]


default_values = {
    "work_outside_street": "",
    "work_outside_city_town": "",
}
