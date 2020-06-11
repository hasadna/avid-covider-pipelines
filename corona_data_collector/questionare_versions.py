import sys
from distutils.version import LooseVersion


questionare_versions = {
    '0.1.0': {'age', 'chronic_cancer', 'chronic_diabetes', 'chronic_hypertension',
              'chronic_ischemic_heart_disease_or_stroke', 'chronic_kidney_failure', 'chronic_lung_disease', 'city_town',
              'continue', 'exposure_met_people', 'flatmates', 'flatmates_over_70', 'flatmates_under_18', 'hospitalized',
              'insulation_exposure_date', 'insulation_patient_number', 'insulation_returned_from_abroad_date',
              'insulation_status', 'sex', 'smoking', 'street', 'symptoms_breath_shortness', 'symptoms_clogged_nose',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_fatigue', 'symptoms_headache', 'symptoms_infirmity',
              'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_sore_throat',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'toplevel_symptoms_stomach',
              'toplevel_symptoms_tiredness', 'version', 'work_serve_over_10'},
    '0.2.0': {'age', 'chronic_cancer', 'chronic_diabetes', 'chronic_hypertension',
              'chronic_ischemic_heart_disease_or_stroke', 'chronic_kidney_failure', 'chronic_lung_disease', 'city_town',
              'continue', 'exposure_met_people', 'flatmates', 'flatmates_over_70', 'flatmates_under_18', 'hospitalized',
              'insulation_exposure_date', 'insulation_patient_number', 'insulation_returned_from_abroad_date',
              'insulation_status', 'locale', 'sex', 'smoking', 'street', 'symptoms_breath_shortness',
              'symptoms_clogged_nose', 'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_fatigue',
              'symptoms_headache', 'symptoms_infirmity', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_sore_throat', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'toplevel_symptoms_stomach', 'toplevel_symptoms_tiredness', 'version',
              'work_serve_over_10'},
    '0.2.1': {'age', 'chronic_cancer', 'chronic_diabetes', 'chronic_hypertension',
              'chronic_ischemic_heart_disease_or_stroke', 'chronic_kidney_failure', 'chronic_lung_disease', 'city_town',
              'continue', 'exposure_met_people', 'flatmates', 'flatmates_over_70', 'flatmates_under_18', 'hospitalized',
              'insulation_exposure_date', 'insulation_patient_number', 'insulation_returned_from_abroad_date',
              'insulation_status', 'locale', 'sex', 'smoking', 'street', 'symptoms_breath_shortness',
              'symptoms_clogged_nose', 'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_fatigue',
              'symptoms_headache', 'symptoms_infirmity', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_sore_throat', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'toplevel_symptoms_stomach', 'toplevel_symptoms_tiredness', 'version',
              'work_serve_over_10', 'xxx'},
    '1.0.1': {'age', 'chronic_cancer', 'chronic_diabetes', 'chronic_hypertension', 'chronic_immune_system_suppression',
              'chronic_ischemic_heart_disease_or_stroke', 'chronic_kidney_failure', 'chronic_lung_disease', 'city_town',
              'dateFirstReport', 'diagnosed_location', 'exposure_status', 'general_feeling', 'insulation_exposure_date',
              'insulation_patient_number', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'locale', 'met_above_18', 'met_under_18', 'numPreviousReports', 'sex', 'smoking',
              'street', 'symptoms_breath_shortness', 'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough',
              'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss',
              'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'toplevel_symptoms_stomach', 'version', 'xxx'},
    '1.1.0': {'age', 'chronic_cancer', 'chronic_diabetes', 'chronic_hypertension', 'chronic_immune_system_suppression',
              'chronic_ischemic_heart_disease_or_stroke', 'chronic_kidney_failure', 'chronic_lung_disease', 'city_town',
              'dateFirstReport', 'diagnosed_location', 'exposure_status', 'general_feeling', 'insulation_exposure_date',
              'insulation_patient_number', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'locale', 'met_above_18', 'met_under_18', 'numPreviousReports', 'sex', 'smoking',
              'street', 'symptoms_breath_shortness', 'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough',
              'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss',
              'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'toplevel_symptoms_stomach', 'version', 'xxx'},
    '2.0.0': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'exposure_status',
              'general_feeling', 'insulation_exposure_date', 'insulation_patient_number', 'insulation_reason',
              'insulation_returned_from_abroad_date', 'insulation_start_date', 'locale', 'met_above_18',
              'met_under_18', 'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression',
              'precondition_chronic_ischemic_heart_disease_or_stroke', 'precondition_chronic_kidney_failure',
              'precondition_chronic_lung_disease', 'precondition_smoking', 'preconditions_received', 'sex', 'street',
              'symptoms_breath_shortness', 'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough',
              'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss',
              'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'version', 'xxx'},
    '2.0.1': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'locale',
              'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version'},
    '2.0.2': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'feel_good', 'general_feeling', 'insulation_exposure_date',
              'insulation_patient_number', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'locale', 'met_above_18', 'met_under_18', 'none', 'notificationsEnabled',
              'numPreviousReports', 'precondition_chronic_cancer', 'precondition_chronic_diabetes',
              'precondition_chronic_hypertension', 'precondition_chronic_immune_system_suppression',
              'precondition_chronic_ischemic_heart_disease_or_stroke', 'precondition_chronic_kidney_failure',
              'precondition_chronic_lung_disease', 'precondition_smoking', 'preconditions_received', 'sex', 'street',
              'symptoms_breath_shortness', 'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough',
              'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss',
              'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'version', 'xxx'},
    '2.0.3': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'feel_good', 'general_feeling', 'insulation_reason',
              'insulation_returned_from_abroad_date', 'insulation_start_date', 'locale', 'met_above_18', 'met_under_18',
              'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version', 'xxx'},
    '2.0.4': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'feel_bad', 'general_feeling', 'insulation_reason',
              'insulation_returned_from_abroad_date', 'insulation_start_date', 'locale', 'met_above_18', 'met_under_18',
              'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version'},
    '2.0.5': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'locale', 'met_above_18', 'met_under_18', 'notificationsEnabled',
              'numPreviousReports', 'precondition_chronic_cancer', 'precondition_chronic_diabetes',
              'precondition_chronic_hypertension', 'precondition_chronic_immune_system_suppression',
              'precondition_chronic_ischemic_heart_disease_or_stroke', 'precondition_chronic_kidney_failure',
              'precondition_chronic_lung_disease', 'precondition_smoking', 'preconditions_received', 'sex', 'street',
              'symptoms_breath_shortness', 'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion',
              'symptoms_diarrhea', 'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough',
              'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss',
              'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough',
              'toplevel_symptoms_pains', 'version'},
    '2.1.0': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'locale',
              'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'recaptcha', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version', 'xxx'},
    '2.2.0': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'FALSE', 'feel_good', 'general_feeling', 'insulation_exposure_date',
              'insulation_patient_number', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'layout', 'locale', 'medical_staff_member', 'met_above_18', 'met_under_18',
              'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'recaptcha', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version'},
    '2.2.2': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'feel_good', 'general_feeling', 'insulation_exposure_date',
              'insulation_patient_number', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'layout', 'locale', 'medical_staff_member', 'met_above_18', 'met_under_18',
              'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'recaptcha', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_chills',
              'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea', 'symptoms_dry_cough',
              'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain', 'symptoms_nausea_and_vomiting',
              'symptoms_smell_taste_loss', 'symptoms_sore_throat', 'symptoms_tiredness_or_fatigue', 'temperature',
              'toplevel_symptoms_cough', 'toplevel_symptoms_pains', 'version'},
    '2.5.4': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_reason', 'insulation_returned_from_abroad_date',
              'insulation_start_date', 'layout', 'locale', 'medical_staff_member', 'met_above_18', 'met_under_18',
              'notificationsEnabled', 'numPreviousReports', 'precondition_chronic_cancer',
              'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'
              },
    '2.5.5': {'age', 'alias', 'city_town', 'dateFirstReport', 'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_reason', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '2.5.6': {'age', 'alias', 'city_town', 'dateFirstReport',
              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '2.5.8': {'age', 'alias', 'city_town', 'dateFirstReport',
              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '2.5.9': {'age', 'alias', 'city_town', 'dateFirstReport',
              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '2.6.0': {'age', 'alias', 'city_town', 'covid19_check_date','covid19_check_result','dateFirstReport',              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '2.7.*': {'age', 'alias', 'city_town', 'covid19_check_date','covid19_check_result','dateFirstReport',              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'version'},
    '3.0.*': {'age', 'alias', 'city_town', 'covid19_check_date','covid19_check_result','dateFirstReport',              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'symptoms_abdominal_pain', "symptoms_lack_of_appetite_or_skipping_meals",
              "routine_workplace_is_outside", "routine_workplace_weekly_hours", "routine_workplace_city_town", "routine_workplace_street",
              "routine_uses_public_transportation", "routine_uses_public_transportation_bus", "routine_uses_public_transportation_train", "routine_uses_public_transportation_taxi", "routine_uses_public_transportation_other",
              "routine_visits_prayer_house", "routine_wears_mask", "routine_wears_gloves", "routine_workplace_single_location",
              'version'},
    '3.1.*': {'age', 'alias', 'city_town', 'covid19_check_date','covid19_check_result','dateFirstReport',              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness',
              'symptoms_chills', 'symptoms_clogged_nose', 'symptoms_confusion', 'symptoms_diarrhea',
              'symptoms_dry_cough', 'symptoms_headache', 'symptoms_moist_cough', 'symptoms_muscles_pain',
              'symptoms_nausea_and_vomiting', 'symptoms_smell_taste_loss', 'symptoms_sore_throat',
              'symptoms_tiredness_or_fatigue', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'symptoms_abdominal_pain', "symptoms_lack_of_appetite_or_skipping_meals",
              "routine_workplace_is_outside", "routine_workplace_weekly_hours", "routine_workplace_city_town", "routine_workplace_street",
              "routine_uses_public_transportation", "routine_uses_public_transportation_bus", "routine_uses_public_transportation_train", "routine_uses_public_transportation_taxi", "routine_uses_public_transportation_other",
              "routine_visits_prayer_house", "routine_wears_mask", "routine_wears_gloves", "routine_workplace_single_location",
              "school_name",
              'version'},
    '4.1.*': {'age', 'alias', 'city_town', 'covid19_check_date','covid19_check_result','dateFirstReport',              'diagnosed_location', 'engagementSource',
              'exposure_status', 'general_feeling', 'insulation_exposure_date', 'insulation_patient_number',
              'insulation_reason', 'insulation_returned_from_abroad_date', 'insulation_start_date', 'layout', 'locale',
              'medical_staff_member', 'met_above_18', 'met_under_18', 'notificationsEnabled', 'numPreviousReports',
              'precondition_chronic_cancer', 'precondition_chronic_diabetes', 'precondition_chronic_hypertension',
              'precondition_chronic_immune_system_suppression', 'precondition_chronic_ischemic_heart_disease_or_stroke',
              'precondition_chronic_kidney_failure', 'precondition_chronic_lung_disease', 'precondition_smoking',
              'preconditions_received', 'served_public_last_fortnight', 'sex', 'street', 'symptoms_breath_shortness', 'symptoms_breath_shortness_duration',
              'symptoms_chills', 'symptoms_chills_duration', 'symptoms_clogged_nose', 'symptoms_clogged_nose_duration', 'symptoms_confusion', 'symptoms_confusion_duration', 'symptoms_diarrhea', 'symptoms_diarrhea_duration',
              'symptoms_dry_cough', 'symptoms_dry_cough_duration', 'symptoms_headache', 'symptoms_headache_duration', 'symptoms_moist_cough', 'symptoms_moist_cough_duration', 'symptoms_muscles_pain', 'symptoms_muscles_pain_duration',
              'symptoms_nausea_and_vomiting', 'symptoms_nausea_and_vomiting_duration', 'symptoms_smell_taste_loss', 'symptoms_smell_taste_loss_duration', 'symptoms_sore_throat', 'symptoms_sore_throat_duration',
              'symptoms_tiredness_or_fatigue', 'symptoms_tiredness_or_fatigue_duration', 'temperature', 'toplevel_symptoms_cough', 'toplevel_symptoms_pains',
              'symptoms_abdominal_pain', 'symptoms_abdominal_pain_duration', "symptoms_lack_of_appetite_or_skipping_meals", "symptoms_lack_of_appetite_or_skipping_meals_duration",
              "routine_workplace_is_outside", "routine_workplace_weekly_hours", "routine_workplace_city_town", "routine_workplace_street",
              "routine_uses_public_transportation", "routine_uses_public_transportation_bus", "routine_uses_public_transportation_train", "routine_uses_public_transportation_taxi", "routine_uses_public_transportation_other",
              "routine_visits_prayer_house", "routine_wears_mask", "routine_wears_gloves", "routine_workplace_single_location",
              "school_name",
              "covid_positive", "covid_last_positive_results_date", "covid_last_negative_results_date", "hospitalization_status", "hospitalization_start_date", "hospitalization_end_date",
              "hospitalization_icu_required", "hospitalization_icu_duration",
              "v4_insulation_status", "v4_insulation_reason",
              'version'},
}
questionare_versions["4.0.*"] = questionare_versions["4.1.*"]

def get_version_columns(target_version):
    target_version = LooseVersion(target_version).version
    for version, columns in questionare_versions.items():
        version = LooseVersion(version).version
        if (
            target_version[0] == version[0] and target_version[1] == version[1]
            and (version[2] == "*" or target_version[2] == version[2])
        ):
            return columns
    raise KeyError("Could not find version " + str(target_version))


def get_last_version():
    version = sorted(map(LooseVersion, questionare_versions.keys()))[-1].version
    return "%s.%s.%s" % (version[0], version[1], "0" if version[2] == "*" else version[2])


def is_supported_version(target_version):
    try:
        get_version_columns(target_version)
        return True
    except KeyError:
        return False


def is_version_larger_or_equal_to(version, target_version):
    try:
        return LooseVersion(version) >= LooseVersion(target_version)
    except Exception:
        return False


if __name__ == "__main__":
    if sys.argv[1] == "--get-version-columns":
        print(get_version_columns(sys.argv[2]))
    elif sys.argv[1] == "--get-last-version":
        print(get_last_version())
    elif sys.argv[1] == "--is-supported-version":
        print(is_supported_version(sys.argv[2]))
    elif sys.argv[1] == "--is_version_larger_or_equal_to":
        print(is_version_larger_or_equal_to(sys.argv[2], sys.argv[3]))
