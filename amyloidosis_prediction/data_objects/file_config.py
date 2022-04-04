"""
Store file configurations for data organization

This file will have to be configured for any new dataset
"""
import os

# The location of original files and split files output directory must be specified
try:
    DIR_RAW = os.environ['AMYLOID_RAW_DIR']
    DIR_SPLIT = os.environ['AMYLOID_SPLIT_DIR']
except:
    raise Exception('Need to specify enviornmental variables: AMYLOID_RAW_DIR and AMYLOID_SPLIT_DIR')

NAME = 'name'
DELIM = 'delimiter'
QUOTECHAR = 'quotechar'
ENCODING = 'encoding'
ID_COL = 'id_column'
COL_DEF = 'column_def'
EXPECTED_ROWS = 'expectedrows'
LOCATION = 'location'
TEXT_COL = 'text_col'
SORT_COL = 'sort_col'


# Define data columns of each dataset, should be specific to dataset and determines the simplh5 HDF5 storage.
NOTE_FILE_COLS = {
  'STUDY_PAT_ID': 's32',
  'STUDY_ENC_ID': 's32',
  'STUDY_NOTE_ID': 's32',
  'NOTE_AUTHOR_NAME': 's80',
  'NOTE_COSIGNER_NAME': 's80',
  'NOTE_TYPE_NAME': 's100',
  'NOTE_DT': 's22',
  'NOTE_CREATE_DT': 's22',
  'NOTE_FILE_DT': 's22',
  'LINKED_ORDER_ID': 's10',
  'NOTE_AUTHOR_SPECIALTY': 's80',
  'NOTE_COSIGNER_SPECIALTY': 's80',
  'RPT_TEXT': 'c360000',
}

CLINICAL_NOTES_DEF = {
    NAME: 'Clinical_Notes',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: NOTE_FILE_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: 'RPT_TEXT',
    SORT_COL: 'NOTE_DT',

}

DIAGNOSES_FILE_COLS = {
  'STUDY_PAT_ID': 's32',
  'STUDY_ENC_ID': 's32',
  'ENCOUNTER_DATE': 's22',
  'DX_NAME': 's260',
  'ICD9_CODE': 's16',
  'ICD9_NAME': 's260',
  'ICD10_CODE': 's16',
  'ICD10_NAME': 's260',
  'DIAGNOSIS_START_DATE': 's22',
  'DIAGNOSIS_END_DATE': 's22',
  'PROBLEM_LIST_FLAG': 's1',
  'PROBLEM_LIST_STATUS': 's2',
  'PRIMARY_DX_FLAG': 's1',
  'BILLING_DX_FLAG': 's1',
  'ENCOUNTER_DX_FLAG': 's1',
  'MEDICAL_HISTORY_DX_FLAG': 's1',
  'ORDERED_PROCEDURE_DX_FLAG': 's1',
  'ORDERED_MEDICATION_DX_FLAG': 's1',
  'HOSPITAL_ADMISSION_DX_FLAG': 's1',
  'REFERRAL_DX_FLAG': 's1',
  'FOLLOWUP_DX_FLAG': 's1',
  'CHRONIC_DX_FLAG': 's1',
  'PRINCIPLE_PROBLEM_DX_FLAG': 's1',
}

DIAGNOSES_DEF = {
    NAME: 'Diagnoses',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: DIAGNOSES_FILE_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['DX_NAME', 'ICD10_NAME', 'ICD9_NAME'],
    SORT_COL: 'DIAGNOSIS_START_DATE',
}

#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Hospital_Encounters.csv
HOSPITAL_ENCOUNTERS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'HOSPITAL_ADMIT_TIME': 's22',
    'HOSPITAL_DISCHARGE_TIME': 's22',
    'INPATIENT_ADMIT_TIME': 's22',
    'INPATIENT_LENGTH_OF_STAY': 's6',
    'FISCAL_LENGTH_OF_STAY': 's4',
    'FISCAL_ICU_DAYS': 's3',
    'OPERATIONAL_ICU_DAYS': 's5',
    'OPERATIONAL_LENGTH_STAY_DAYS': 's6',
    'ADMITTING_DX_ICD_CODE': 's50',
    'ADMITTING_DX_NAME': 's260',
    'BILL_DX1_ICD10_CODE': 's50',
    'BILL_DX1_ICD10_NAME': 's260',
    'BILL_DX1_ICD9_CODE': 's50',
    'BILL_DX1_ICD9_NAME': 's260',
    'BILL_DX2_ICD10_CODE': 's50',
    'BILL_DX2_ICD10_NAME': 's260',
    'BILL_DX2_ICD9_CODE': 's50',
    'BILL_DX2_ICD9_NAME': 's260',
    'BILL_DX3_ICD10_CODE': 's50',
    'BILL_DX3_ICD10_NAME': 's260',
    'BILL_DX3_ICD9_CODE': 's50',
    'BILL_DX3_ICD9_NAME': 's260',
    'BILL_DX4_ICD10_CODE': 's50',
    'BILL_DX4_ICD10_NAME': 's260',
    'BILL_DX4_ICD9_CODE': 's50',
    'BILL_DX4_ICD9_NAME': 's260',
    'ENCOUNTER_ICD10_DIAGNOSES': 's5000',
    'ENCOUNTER_ICD9_DIAGNOSES': 's5000',
    'ENCOUNTER_PROCEDURES': 's5000',
    'ADMITTED_FROM_ED_YN': 's1',
    'CHIEF_COMPLAINT': 's100',
    'ADMISSION_SOURCE': 's100',
    'DISCHARGE_LOCATION': 's30',
    'DISCHARGE_DISPOSITION': 's100',
    'AIRWAY_DEVICE': 's1',
    'READMISSION': 's1',
    'READMISSION_WITHIN_72_HOURS': 's1',
    'SUBSEQUENT_ADMIT_WITHIN_30DAYS': 's1',
    'DAYS_UNTIL_READMISSION': 's6',
    'PRIOR_FISCAL_LENGTH_OF_STAY': 's4',
    'ENCOUNTER_VISIT_TYPE': 's26',
    'MORTALITY_YN': 's1',
    'PALLIATIVE_CARE_ORDER_YN': 's1',
    'PATIENT_TYPE': 's20',
    'PAT_STATUS': 's100',
    'TRANSFER_YN': 's1',
    'TRANSFER_FROM': 's50',
}

HOSPITAL_DEF = {
    NAME: 'Hospital_Encounters',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: HOSPITAL_ENCOUNTERS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
}

#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Administered_Medications.csv
ADMIN_MEDICATIONS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'RX_DESCRIPTION': 's200',
    'RX_DISPLAY_NAME': 's300',
    'RX_ORDER_TO_START_TIME': 's22',
    'RX_ORDER_TO_END_TIME': 's22',
    'RX_ORDER_PLACED': 's10',
    'RX_ORDER_STARTED': 's10',
    'ROUTE': 's20',
    'DOSE': 's11',
    'FREQUENCY': 's34',
    'RX_TAKEN_TIME': 's22',
    'MAR_RESULT': 's18',
}

ADMIN_MEDICATIONS_DEF = {
    NAME: 'Administered_Medications',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: ADMIN_MEDICATIONS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
}

#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Ambulatory_Encounters.csv
AMBULATORY_ENCOUNTERS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'AGE_YRS': 's6',
    'ENC_DATE': 's22',
    'ENC_TYPE': 's36',
    'DEPT_NAME': 's40',
    'DEPT_ID': 's11',
    'INSURANCE_PAYOR': 's50',
    'INSURANCE_CLASS': 's40',
    'INSURANCE_GROUP': 's8',
    'ENC_PROVIDER': 's32',
    'ENC_PROVIDER_SPECIALTY1': 's60',
}
AMBULATORY_ENCOUNTERS_DEF = {
    NAME: 'Ambulatory_Encounters',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: AMBULATORY_ENCOUNTERS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
}


#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Current_Medications.csv
CURRENT_MEDICATIONS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'LAST_CURRENT_MEDS_DATE': 's10',
    'MED_NAME': 's100',
    'GENERIC_NAME_1': 's50',
    'GENERIC_NAME_2': 's100',
    'PHARM_CLASS_NAME': 's50',
    'PHARM_SUBCLASS_NAME': 's70',
    'THERAPY_CLASS_NAME': 's50',
    'ADMINISTRATION_ROUTE': 's20',
}
CURRENT_MEDICATIONS_DEF = {
    NAME: 'Current_Medications',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: CURRENT_MEDICATIONS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['GENERIC_NAME_1', 'GENERIC_NAME_2', 'PHARM_CLASS_NAME', 'PHARM_SUBCLASS_NAME'],
    SORT_COL: 'LAST_CURRENT_MEDS_DATE',
}

#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Ordered_Medications.csv
ORDERED_MEDICATIONS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'ENCOUNTER_DATE': 's10',
    'RX_ORDER_PLACED_TIME': 's10',
    'RX_ORDER_START_TIME': 's10',
    'RX_ORDER_END_TIME': 's10',
    'RX_ORDER_TO_START_TIME': 's22',
    'RX_ORDER_TO_END_TIME': 's22',
    'RX_ORDER_DISCONTINUE_TIME': 's22',
    'RX_ORDER_MODE': 's10',
    'MED_NAME': 's150',
    'GENERIC_NAME_1': 's50',
    'GENERIC_NAME_2': 's100',
    'PHARM_CLASS_NAME': 's50',
    'PHARM_SUBCLASS_NAME': 's100',
    'THERAPY_CLASS_NAME': 's50',
    'ADMINISTRATION_ROUTE': 's21',
    'DOSE': 's20',
    'FREQUENCY': 's34',
    'UNIT': 's17',
    'MED_ORDER_STATUS': 's12',
}
ORDERED_MEDICATIONS_DEF = {
    NAME: 'Ordered_Medications',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: ORDERED_MEDICATIONS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['GENERIC_NAME_1', 'GENERIC_NAME_2', 'PHARM_CLASS_NAME', 'PHARM_SUBCLASS_NAME', 'THERAPY_CLASS_NAME'],
    SORT_COL: 'ENCOUNTER_DATE'
}

#S:/NLP/Data/Masri_Elman_IRB21240_File_Drop\Masri_Elman_IRB21240_Demographics.csv
DEMOGRAPHICS_COLS = {
    'STUDY_PAT_ID': 's32',
    'MRN_CD': 's8',
    'CURR_AGE_YRS': 's3',
    'BIRTH_DT': 's10',
    'GENDER_NM': 's7',
    'NIH_ETHNCTY': 's22',
    'NIH_RACE': 's23',
    'PT_VIABLE_FLG': 's1',
    'DEATH_DT': 's10',
    'LAST_NM': 's25',
    'FIRST_NM': 's20',
    'CURR_PCP': 's60',
    'BIO_SAMPL_OPT_OUT_FLG': 's1',
    'GENETC_OPT_OUT_FLG': 's1',
}
DEMOGRAPHICS_DEF = {
    NAME: 'Demographics',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: DEMOGRAPHICS_COLS,
    EXPECTED_ROWS: 100000,
    LOCATION: DIR_RAW,
}

ORDER_NARRATIVE_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'STUDY_ORDER_ID': 's32',
    'ORDER_DATE': 's16',
    'RESULT_DATE': 's16',
    'PROC_NAME': 's100',
    'NARRATIVE_TEXT': 's500000',
}
ORDER_NARRATIVE_DEF = {
    NAME: 'Order_Narrative',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'latin-1',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: ORDER_NARRATIVE_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: 'NARRATIVE_TEXT',
    SORT_COL: 'ORDER_DATE',
}




LAB_RESULTS_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'STUDY_ORDER_ID': 's32',
    'SPECIMEN_TAKEN_TIME': 's22',
    'PROCEDURE_NAME': 's100',
    'COMPONENT_NAME': 's100',
    'RESULT_NUMBER': 's12',
    'RESULT_EQ_FLAG': 's3',
    'RESULT_TEXT': 's1000',
    'ORDER_TIME': 's22',
    'RESULT_TIME': 's22',
    'REFERENCE_UNIT': 's30',
    'REFERENCE_MINIMUM': 's20',
    'REFERENCE_MAXIMUM': 's20',
}
LAB_RESULTS_DEF = {
    NAME: 'Lab_Results',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: LAB_RESULTS_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['PROCEDURE_NAME', 'COMPONENT_NAME', 'RESULT_TEXT'],
    SORT_COL: 'ORDER_TIME',
}


RESULT_NOTES_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'SPECIMEN_DATE': 's16',
    'PROC_NAME': 's100',
    'COMMENT_TEXT': 's100000',
}
RESULT_NOTES_DEF = {
    NAME: 'Result_Notes',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: RESULT_NOTES_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['COMMENT_TEXT', 'PROC_NAME'],
    SORT_COL: 'SPECIMEN_DATE',
}



PATHOLOGY_COLS = {
    'STUDY_PAT_ID': 's32',
    'STUDY_ENC_ID': 's32',
    'STUDY_CASE_ID': 's32',
    'STUDY_RESULT_ID': 's32',
    'STUDY_ORDER_ID': 's32',
    'SOURCE_SYSTEM': 's8',
    'SPECIMEN_COLL_DT': 's10',
    'RESULT_STATUS': 's8',
    'LAB_STATUS': 's21',
    'RESULT_COMPLETED_DT': 's10',
    'REPORT_HEADER': 's30',
    'REPORT_GROUP': 's2',
    'REPORT_LINE': 's2',
    'REPORT_BODY_LINE_OF_TEXT': 's300',
}
PATHOLOGY_DEF = {
    NAME: 'Pathology',
    DELIM: '|',
    QUOTECHAR:'"',
    ENCODING: 'utf-8',
    ID_COL: 'STUDY_PAT_ID',
    COL_DEF: PATHOLOGY_COLS,
    EXPECTED_ROWS: 30000000,
    LOCATION: DIR_RAW,
    TEXT_COL: ['REPORT_BODY_LINE_OF_TEXT'],
    SORT_COL: 'RESULT_COMPLETED_DT',
}
