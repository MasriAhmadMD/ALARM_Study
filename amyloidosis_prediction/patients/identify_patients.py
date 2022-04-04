
import logging
import os
import re


from amyloidosis_prediction.data_objects.base_obj_common import BaseObjCommon, get_model_dir
from amyloidosis_prediction.data_objects.file_config import DIAGNOSES_DEF, DEMOGRAPHICS_DEF, CLINICAL_NOTES_DEF, DIR_RAW



def patients_clinical_note_dates():

    outdir = os.path.join(DIR_RAW, 'analysis_output')
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    bobj = BaseObjCommon(CLINICAL_NOTES_DEF)
    cols = [
        bobj.id_col,
        'NOTE_DT'
    ]

    pat_info = {}
    for row in bobj.iter_split_rows(cols=cols, as_dict=True):
        pat = row[bobj.id_col]
        dt = row['NOTE_DT']
        if pat not in pat_info:
            pat_info[pat] = set()
        pat_info[pat].add(dt)


    with open(os.path.join(outdir, 'clinical_note_dates.txt'), 'w') as f:
        for pat, dts in pat_info.items():
            dates = ','.join(sorted(dts))
            f.write(f'{pat},{dates}\n')



def group_hf_neuro_amy_patients():

    ICD_10 = 'ICD10_CODE'
    ICD_9 = 'ICD9_CODE'
    bobj = BaseObjCommon(DIAGNOSES_DEF)

    groups = {
        'heartfailure': {
            ICD_10: ['I0981', 'I50'],
            ICD_9: ['39891', '40201', '40211', '40291', '40401', '40403', '40411', '40413',
                    '40491', '40493', '425', '428'],
        },
        'neuropathy': {
            ICD_10: ['G54', 'G55', 'G56', 'G57', 'G58', 'G59', 'G60', 'G61', 'G62', 'G63', 'G64', 'G90', 'G990'],
            ICD_9: ['337', '353', '354', '355', '356', '357'],
        },
        'amyloidosis': {
            ICD_10: ['E85'],
            ICD_9: ['2773'],
        },
    }

    cols = [
        bobj.id_col,
        ICD_10,
        ICD_9,
    ]

    non_alphanumeric = re.compile("[^0-9a-zA-Z]+")
    patients = {g: set() for g in groups}
    for group in groups:

        icd9_match = re.compile('|'.join(groups[group][ICD_9]))
        icd10_match = re.compile('|'.join(groups[group][ICD_10]))

        for row in bobj.iter_split_rows(cols=cols, as_dict=True):

            patient_id = row[bobj.id_col]
            icd9cd = non_alphanumeric.sub('', row[ICD_9])
            icd10cd = non_alphanumeric.sub('', row[ICD_10])

            if re.match(icd10_match, icd10cd) or re.match(icd9_match, icd9cd):
                patients[group].add(patient_id)


    for group in patients:
        #outfile = f'../patients/{group}_patients_icd9_icd10.txt'
        outfile = f'{group}_patients_icd9_icd10.txt'
        with open(outfile, 'w') as f:
            for pat in patients[group]:
                f.write(f'{pat}\n')


if __name__ == '__main__':

    patients_clinical_note_dates()
    group_hf_neuro_amy_patients()

