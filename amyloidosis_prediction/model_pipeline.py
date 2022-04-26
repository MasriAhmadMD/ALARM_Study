"""
Full modelling pipeline to generate data and results.
"""

from datetime import datetime
import glob
import os
import logging
import numpy as np
import re

from simpleh5 import H5ColStore

from amyloidosis_prediction.data_objects.base_obj_common import BaseObjCommon, get_model_dir
from amyloidosis_prediction.data_objects.file_config import NAME, DIR_RAW, DIR_SPLIT, \
    CLINICAL_NOTES_DEF, DIAGNOSES_DEF, LAB_RESULTS_DEF, RESULT_NOTES_DEF, \
    HOSPITAL_DEF, ADMIN_MEDICATIONS_DEF, AMBULATORY_ENCOUNTERS_DEF, CURRENT_MEDICATIONS_DEF, ORDERED_MEDICATIONS_DEF, \
    PATHOLOGY_DEF, ORDER_NARRATIVE_DEF
from amyloidosis_prediction.utility.csv_split import split_csvs, csv_split_to_hdf5_split

from amyloidosis_prediction.models.run_models import train_check_models
from amyloidosis_prediction.patients.characterize_patient_dates import characterize_and_return_dates
from amyloidosis_prediction.patients.identify_patients import patients_clinical_note_dates, group_hf_neuro_amy_patients, ICD_9, ICD_10


def get_vec_data(table_data: dict, valid_patients: set, odds_ratio: int, num_topics: int,
                 model_tables: tuple=(), name='', restrict_patients=None):
    """
    Get the data for individual combinations of data into X train/test matrices and y train/test vectors.

    :param table_data:
    :param valid_patients:
    :param model_tables:
    :return:
    """

    columns = []
    X_train = []
    X_test = []
    y_train = []
    y_test = []
    pat_train = []
    pat_test = []

    bobj = BaseObjCommon(DIAGNOSES_DEF, odds_ratio=odds_ratio, num_topics=num_topics, name=name, restrict_patients=restrict_patients)
    amyloid_pats = bobj.get_amyloid_pat_ids()

    for pcnt, pat_id in enumerate(sorted(valid_patients)):
        group = bobj.pat_group_string(pat_id)
        target = 0
        if pat_id in amyloid_pats:
            target = 1

        if bobj._is_test_group(group):
            pat_test.append(pat_id)
            y_test.append(target)
        else:
            pat_train.append(pat_id)
            y_train.append(target)

        row = []
        for i, (table_name, data) in enumerate(table_data.items()):

            if model_tables and table_name not in model_tables:
                continue
            row.extend(data['patients'][pat_id])
            if pcnt == 0:
                # create columns one time
                columns.extend(data['columns'])

        if bobj._is_test_group(group):
            X_test.append(row)
        else:
            X_train.append(row)

    X_train = np.array(X_train)
    X_test = np.array(X_test)
    y_train = np.array(y_train)
    y_test = np.array(y_test)

    modelling_data = {
        'X_train': X_train,
        'X_test': X_test,
        'y_train': y_train,
        'y_test': y_test,
        'columns': columns,
        f'train_{bobj.id_col}': pat_train,
        f'test_{bobj.id_col}': pat_test,
    }

    return modelling_data


def false_positive_overlap(run_name: str, odds_ratio: int, num_topics: int, name=''):
    """Create a file of patients between data files that are all false positives"""

    model_dir = get_model_dir(odds_ratio, num_topics, name=name)
    results_dir = os.path.join(model_dir, run_name)
    fp_files = glob.glob(os.path.join(results_dir, '*_false_positives_*.txt'))
    fps_sets = {}
    for f in fp_files:
        base = re.sub('\.txt', '', os.path.basename(f))
        fps_sets[base] = set(open(f).read().strip().split('\n'))

    # peform overlap on all combinations of files and record
    with open(os.path.join(results_dir, 'fp_overlap.csv'), 'w', newline='') as f:

        a = [n for n in fps_sets]
        f.write(f'{",".join(a)}\n')
        for name1, fp1 in fps_sets.items():
            f.write(f'{name1}')
            for name2, fp2 in fps_sets.items():
                overlap = fp1.intersection(fp2)
                p = len(overlap) / len(fp1) * 100
                f.write(f',{p}')
            f.write('\n')


def get_model_data(run_list, vec_type, odds_ratio, num_topics, name='', restrict_patients=None):

    model_data = {}
    for file_def in run_list:
        bobj = BaseObjCommon(file_def, odds_ratio=odds_ratio, num_topics=num_topics, name=name, restrict_patients=restrict_patients)
        model_data[bobj.table_name] = bobj.get_model_vectors(vec_type=vec_type)

    # get overlapping patient ids
    overlap_patients = set()
    for i, (table_name, data) in enumerate(model_data.items()):
        pats = set(data['patients'])
        if i == 0:
            overlap_patients = pats
        else:
            overlap_patients = overlap_patients.intersection(pats)

    return model_data, overlap_patients


def train_models(run_list, odds_ratio, num_topics, name: str='', restrict_patients=None):

    tm = datetime.now().strftime('%Y_%m_%d_%H_%M')
    table_list = "-".join([x[NAME] for x in run_list])
    run_name = f'{tm}_{table_list}{name}'

    model_dir = get_model_dir(odds_ratio, num_topics, name=name)
    logging.info(f'**** Traning info ****')
    logging.info(f'  Output_dir: {model_dir}')
    logging.info(f'  Training: {run_name}')
    logging.info(f'  Odds: {odds_ratio} Num_topics: {num_topics}  Table list: {table_list}')
    if restrict_patients:
        logging.info(f'  Restricted patients: {len(restrict_patients)}')
    else:
        logging.info(f'  No restricted patients')

    individual_tables = True
    print_pats = False
    model_types = ['lsi']
    for vnum, vec_type in enumerate(model_types):

        model_data, overlap_patients = get_model_data(run_list, vec_type, odds_ratio, num_topics, name=name, restrict_patients=restrict_patients)

        names = [table_name for table_name in model_data]
        logging.info(f'Integrating {len(names)} datasets: {names}')
        logging.info(f'Number overlapping patients: {len(overlap_patients)}')

        train_data = get_vec_data(model_data, overlap_patients, odds_ratio, num_topics, model_tables=(), name=name, restrict_patients=restrict_patients)

        doinit = False
        if vnum == 0:
            doinit = True
        # run modelling on all tables
        train_check_models(train_data, run_name, table_names=f'{vec_type}_all', file_defs=run_list,
                           odds_ratio=odds_ratio, num_topics=num_topics, init=doinit, print_pats=print_pats,
                           name=name)

        # run each individual table
        if individual_tables:
            for i, file_def in enumerate(run_list):
                table_name = file_def[NAME]
                train_data = get_vec_data(model_data, overlap_patients, odds_ratio, num_topics, model_tables=(table_name,),
                                          name=name, restrict_patients=restrict_patients)
                train_check_models(train_data, run_name, table_names=f'{vec_type}_{table_name}',
                                   file_defs=[file_def], odds_ratio=odds_ratio, num_topics=num_topics, print_pats=print_pats,
                                   name=name)

        false_positive_overlap(run_name, odds_ratio, num_topics, name=name)


def preprocess_data(run_data_list, limit_rows=0):
    """
    convert all csv to hdf5 and clean split text column to be used

    :param run_data_list:  list of data file definitions to preprocess
    :param limit_rows: can limit rows for initial testing so all data is not generated, can make everything run fast for testing
    :return:
    """

    for j, file_def in enumerate(run_data_list):

        input_directory = DIR_RAW
        output_directory = DIR_SPLIT
        split_csvs(input_directory, output_directory, file_def, limit_rows=limit_rows)
        csv_split_to_hdf5_split(output_directory, output_directory, file_def)

        bobj = BaseObjCommon(file_def)
        bobj.create_clean_text_split()


def run_pipeline(
        run_data_list = (DIAGNOSES_DEF, CLINICAL_NOTES_DEF), # , LAB_RESULTS_DEF, RESULT_NOTES_DEF),
        num_lsi_topics=(100,),
        check_odds_ratios=(3.5,),
        limit_rows: int=0,
        generate_data: bool=True,
):
    """
    Run the full pipeline against a set of different clinical datasets across parameters.

    :param run_data_list: list of clinical data sets to perform modelling against
    :param num_lsi_topics:  list of number of lsi topics.  Each will perform full LSI generation with that number.
    :param check_odds_ratios:  list of different odds ratios to generate models against.
    :param limit_rows: sets the number of rows of initial data to setup to be processed.   if set to 0 will run all data.  Can be used to perform quick experiments.
    :return:
    """

    # perform preprocessing on csv data to convert and split into HDF5 files
    preprocess_data(run_data_list, limit_rows=limit_rows)

    # determine dates of clinical notes to restrict/clean up based on date and counts of clinical notes
    patients_clinical_note_dates()
    date_restricted_patients = characterize_and_return_dates(print_stats=False)

    # generate groups of amyloidosis patients based on icd9 and icd10 codes
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
    bobj = BaseObjCommon(DIAGNOSES_DEF)
    group_hf_neuro_amy_patients(bobj, groups)

    # get sets of patients determiented by icd codes
    all_amy_patients = set(open(os.path.join(bobj.dir_out, 'amyloidosis_patients_icd9_icd10.txt')).read().strip().split('\n'))
    hf_patients = set(open(os.path.join(bobj.dir_out, 'heartfailure_patients_icd9_icd10.txt')).read().strip().split('\n'))
    neuro_patients = set(open(os.path.join(bobj.dir_out, 'neuropathy_patients_icd9_icd10.txt')).read().strip().split('\n'))
    hfneuro_patients = hf_patients.union(neuro_patients)

    patient_subgroup = {
        'all2': None,  # all patients
        'hfneuro2': hfneuro_patients,  # only heart failure and neurology patients
        'hf2': hf_patients,  # only heart failure patients
        'neuro2': neuro_patients,  # only neurology patients
    }

    for dorestrict in ['restrict2', '']:  # two scenarios, restrict patients by precomputed date restrictions or not
        for subgroup, orig_restrict_patients in patient_subgroup.items():

            if dorestrict:
                subgroupname = f'{dorestrict}_{subgroup}'
                if orig_restrict_patients:
                    restrict_patients = orig_restrict_patients.intersection(date_restricted_patients)
                else:
                    restrict_patients = date_restricted_patients
                amy_patients = all_amy_patients.intersection(date_restricted_patients)
            else:
                subgroupname = f'{subgroup}'
                restrict_patients = orig_restrict_patients
                amy_patients = all_amy_patients

            # debug logging
            logging.info(f'## Restrict patients: ({dorestrict})')
            if restrict_patients:
                logging.info(f'  Restrict len: {len(restrict_patients)} ')
            else:
                logging.info(f'  Restrict len: {restrict_patients} ')

            for num_topics in num_lsi_topics:  # could add additional topics here
                for odds_ratio in check_odds_ratios:  # can add additional

                    if generate_data:  # utility flag to skip generation of data if already generated
                        for j, file_def in enumerate(run_data_list):

                            bobj = BaseObjCommon(file_def, num_topics=num_topics, odds_ratio=odds_ratio,
                                                 name=subgroupname, restrict_patients=restrict_patients,
                                                 amyloid_pats=amy_patients)

                            amy_exclude_frac = 0.85  # exclude common target words
                            other_exclude_frac = 0.6 # exclude common other words
                            if not bobj.token_counts_exist():
                                bobj.create_token_counts()
                            if not bobj.dictionary_exists():
                                bobj.create_token_dictionary(amy_exclude_frac=amy_exclude_frac,
                                                             other_exclude_frac=other_exclude_frac,
                                                             min_amy_cnt=10,
                                                             min_other_cnt=50,
                                                             top_n_amy=10000,
                                                             total_words=50000,
                                                             )
                            if not bobj.bow_exists():
                                bobj.create_bow()
                            if not bobj.lsivecs_exist():
                                bobj.create_lsi_vecs()

                    train_models(run_data_list, odds_ratio, num_topics, name=subgroupname, restrict_patients=restrict_patients)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    generate_lsi_vec_data = False
    run_pipeline(limit_rows=20000, generate_data=generate_lsi_vec_data)  # set limit rows to 0 to run all data
