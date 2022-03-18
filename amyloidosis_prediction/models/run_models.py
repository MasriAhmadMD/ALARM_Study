"""
Precision == Specificity
Recall == Sensitivity
"""

import pickle
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from xgboost import XGBClassifier

import logging
import numpy as np
import os

# from ohsu_amyloidosis.pipelines.data_objects.diagnoses import Diagnoses, E85_FLAG
from amyloidosis_prediction.data_objects.base_obj_common import BaseObjCommon, get_model_dir
from amyloidosis_prediction.data_objects.file_config import NAME, TEXT_COL

ID_COL = 'STUDY_PAT_ID'
CAT_COLS = 'cat_cols'
DUMMY_COLS = 'dummy_cols'

TARGET_NAMES = ['none', 'amylodosis']


def print_patients(patient_list, file_defs, filename, doc_type):

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        for pat_id in patient_list:
            f.write(f'\n*********PATID: {pat_id} ************\n')
            for file_def in file_defs:
                f.write(f'######### Data: {file_def[NAME]} : {file_def[TEXT_COL]} ############\n')

                obj = BaseObjCommon(file_def)
                docs = obj.get_patient_document(pat_id)
                f.write(f'{docs[doc_type]}\n')
            f.write(f'\n*********END PATID: {pat_id} ************\n')



def train_check_models(data: dict,
                       run_name: str,
                       table_names: str, file_defs: list, odds_ratio: int, num_topics: int, init: bool=False,
                       print_pats: bool=False, scale_pos_weight=200, name=''):

    if not run_name:
        raise Exception('give the model a run name')

    model_dir = get_model_dir(odds_ratio, num_topics, name=name)
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)

    # print all false positive patients into csv and print out clinical notes
    results_dir = os.path.join(model_dir, run_name)
    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    logging.info(f"Training information: {run_name}")
    for x in data:
        logging.info(f' {x}: {type(data[x])}')
        if isinstance(x, np.ndarray):
            logging.info(f'   {np.shape(data[x])}')

    X_train = data['X_train']
    X_test = data['X_test']
    y_train = data['y_train']
    y_test = data['y_test']

    train_pat_ids = data[f'train_{ID_COL}']
    test_pat_ids = data[f'test_{ID_COL}']
    columns = data['columns']

    cat_cols = data.get(CAT_COLS, [])
    dummy_columns = data.get(DUMMY_COLS, [])
    logging.info(f'Model Categories ({len(cat_cols)}): {cat_cols}')
    logging.info(f'Model columns ({len(columns)}): {columns}')
    logging.info(f'Model dummy_columns ({len(dummy_columns)}): {dummy_columns}')

    logging.info("Check types")
    logging.info(f'  X_train: {type(X_train)}, {np.shape(X_train)}, {X_train.dtype}')
    logging.info(f'  y_train: {type(y_train)}, {np.shape(y_train)}, {y_train.dtype}')
    logging.info(f'  X_test: {type(X_test)}, {np.shape(X_test)}, {X_test.dtype}')
    logging.info(f'  y_test: {type(y_test)}, {np.shape(y_test)}, {y_test.dtype}')

    MODELS = {
        #'xgboost10': XGBClassifier(scale_pos_weight=10),
        #'xgboost100': XGBClassifier(scale_pos_weight=100),
        #'xgboost200': XGBClassifier(scale_pos_weight=200),
        'xgboost400': XGBClassifier(scale_pos_weight=400),
        #'LogReg': LogisticRegression(solver='liblinear'),
        #'KNN': KNeighborsClassifier(n_neighbors=10),
        #'RF': RandomForestClassifier(),
        #'SVM': SVC(),
        #'LASSOCV': LassoCV(),
    }

    for j, (name, model) in enumerate(MODELS.items()):

        logging.info(f"Training {name}....")
        logging.info(f'   Fit training model...')
        clf = model.fit(X_train, y_train)
        try:
            with open(os.path.join(results_dir, f'xgboost_model_odds_{odds_ratio}_topics_{num_topics}.pkl'), 'w') as f:
                pickle.dump(clf, f)
        except:
            with open('error_file.txt', 'a') as f:
                f.write(f'Could not save model {name} odds:{odds_ratio} topics: {num_topics}')

        y_pred = clf.predict(X_test)
        tp = 0
        fp = 0
        tn = 0
        fn = 0
        fn_patients = []
        fp_patients = []
        tp_patients = []
        for i, pred in enumerate(y_pred):
            pat_id = test_pat_ids[i]
            truth = y_test[i]

            if truth and pred:
                tp += 1
                tp_patients.append(pat_id)
            elif truth and not pred:
                fn += 1
                fn_patients.append(pat_id)
            elif not truth and pred:
                fp += 1
                fp_patients.append(pat_id)
            elif not truth and not pred:
                tn += 1
            else:
                raise Exception(f'Strange truth and prediction: truth={truth} pred={pred}')

        logging.info(f' TP: {tp}')
        logging.info(f' FP: {fp}')
        logging.info(f' TN: {tn}')
        logging.info(f' FN: {fn}')

        y_train_pred = clf.predict(X_train)
        train_fp_patients = []
        for i, pred in enumerate(y_train_pred):
            pat_id = train_pat_ids[i]
            truth = y_train[i]
            if not truth and pred:
                train_fp_patients.append(pat_id)

        specificity = 0
        if (tn + fp) > 0:
            specificity = tn / (tn + fp)
        sensitivity = 0
        if (tp + fn) > 0:
            sensitivity = tp / (tp + fn)
        logging.info(f' Sensitivity: {sensitivity}')
        logging.info(f' Specificity: {specificity}')
        logging.info(classification_report(y_test, y_pred)) #, target_names=TARGET_NAMES))

        if init and j==0:
            with open(os.path.join(results_dir, f'training_patient_list.txt'), 'w', newline='') as f:
                f.write('\n'.join(train_pat_ids))

            with open(os.path.join(results_dir, f'test_patient_list.txt'), 'w', newline='') as f:
                f.write('\n'.join(train_pat_ids))

            #with open(os.path.join(results_dir, f'columns.txt'), 'w', newline='') as f:
            #    f.write('\n'.join(columns))

            with open(os.path.join(results_dir, f'run_info.txt'), 'w', newline='') as f:
                f.write(f"Number Training Patients,{len(train_pat_ids)}")
                f.write(f"Number Test Patients,{len(test_pat_ids)}")
                f.write(f"Models run,{','.join(MODELS)}")
                f.write(f'Scale_pos_weight,{scale_pos_weight}')

            with open(os.path.join(results_dir, f'{run_name}_model_results.csv'), 'w', newline='') as f:
                row_str = ",".join(['run_name', 'model', 'tables', 'num_columns', 'TP', 'FP', 'TN', 'FN', 'SENSITIVITY', 'SPECIFICITY'])
                f.write(f"{row_str}\n")

        with open(os.path.join(results_dir, f'{run_name}_model_results.csv'), 'a', newline='') as f:
            row_str = ",".join([str(x) for x in [run_name, name, table_names, len(columns), tp, fp, tn, fn, sensitivity, specificity]])
            f.write(f"{row_str}\n")

        with open(os.path.join(results_dir, f'{name}_false_positives_{table_names}.txt'), 'w', newline='') as f:
            for pat_id in fp_patients:
                f.write(f'{pat_id}\n')

        with open(os.path.join(results_dir, f'{name}_false_negatives_{table_names}.txt'), 'w', newline='') as f:
            for pat_id in fn_patients:
                f.write(f'{pat_id}\n')

        with open(os.path.join(results_dir, f'{name}_false_positives_train_{table_names}.txt'), 'w', newline='') as f:
            for pat_id in train_fp_patients:
                f.write(f'{pat_id}\n')

        if print_pats:
            #print_patients(fp_patients, file_defs, os.path.join(results_dir, f'fp_raw_{name}_{table_names}.txt'), 'raw')
            print_patients(fp_patients, file_defs, os.path.join(results_dir, f'fp_{name}_{table_names}.txt'), 'clean')
            print_patients(fn_patients, file_defs, os.path.join(results_dir, f'fn_{name}_{table_names}.txt'), 'clean')
            #print_patients(train_fp_patients, file_defs, os.path.join(results_dir, f'fp_train_{name}_{table_names}.txt'), 'clean')
            #print_patients(tp_patients, file_defs, os.path.join(results_dir, f'tp_{name}_{table_names}.txt'), 'clean')
            #print_patients(tp_patients, file_defs, os.path.join(results_dir, f'tp_raw_{name}_{table_names}.txt'), 'raw')

        if name == 'LogReg':
            logging.info('Logistic Coefficients...')
            with open(os.path.join(results_dir, f'{name}_coef_{table_names}.txt'), 'w', newline='') as f:
                f.write(f'column_name,coefficient\n')
                for i, col in enumerate(columns):
                    coeff = model.coef_[0][i]
                    logging.info(f'  {col}: {coeff}')
                    f.write(f'{col},{coeff}\n')