"""
Base object common methods
"""

import copy
import csv
from datetime import datetime
import gensim
import glob
import hashlib
import logging
import os
import numpy as np
import re
import time
from collections import Counter
from typing import Any, Optional, Tuple, Union

from simpleh5 import H5ColStore
from simpleh5.utilities.serialize_utilities import obj_dtype

from amyloidosis_prediction.data_objects.file_config import NAME, ID_COL, TEXT_COL, SORT_COL, DIR_SPLIT, DIR_RAW
from amyloidosis_prediction.utility.timing import log_time

PAT_INDEX = 'patient_index'
PAT_LOOKUP = 'patient_lookup'
NUM_PATIENTS = 'num_patients'
PAT_FLAGS_TBL = 'patient_flags_table'

BOW = 'bow'
GROUP = 'group'
VECS = 'vecs'


# generate cleaning regular expressions
is_space = re.compile('\s+', re.MULTILINE)
is_not_a_z = re.compile('[^a-z]+')
is_web = re.compile('(^http)|(^www)|(\.edu$)|(\.com$)|(\.org$)')


def simple_clean(text):
    text = re.sub('\s+', '_', text.lower().strip())
    return text


def get_odds_dir(odds_ratio, name: str=''):

    oddsname = str(odds_ratio).replace('.', '_')
    odds_directory = os.path.join(DIR_SPLIT, f'odds_{oddsname}{name}')
    return odds_directory


def get_model_dir(odds_ratio, num_topics, name: str=''):
    odds_directory = get_odds_dir(odds_ratio, name=name)
    oddsname = str(odds_ratio).replace('.', '_')
    model_name = f'odds_{oddsname}_topics_{num_topics}{name}'
    model_dir = os.path.join(odds_directory, model_name)
    return model_dir


class BaseObjCommon(object):

    def __init__(self, file_def: dict, name: str='', num_topics: int=0, odds_ratio: Union[int, float]=0,
                 restrict_patients: set=None, amyloid_pats: set=None, **kwargs):

        dir_split = DIR_SPLIT
        self.dir_out = DIR_SPLIT
        self.dir_raw = DIR_RAW
        dir_raw = DIR_RAW

        self._name = name
        self._file_def = file_def
        self._restrict_patients = restrict_patients
        self._amyloid_pats = set()
        if amyloid_pats:
            self._amyloid_pats = set(amyloid_pats)

        if not self._restrict_patients:
            self._restrict_patients = set()
        self.table_name = self._file_def[NAME]
        self.id_col = self._file_def[ID_COL]
        self.enc_id_col = 'STUDY_ENC_ID'
        self.num_groups = 10000

        # are the split files in a single file or in a directory of individual files
        self._single_file = False
        if self.table_name in {'Clinical_Notes'}:
            self._single_file = False

        self._cur_dir = os.path.dirname(os.path.abspath(__file__))
        self._data_dir = os.path.join(self._cur_dir, '../data')

        self._flag_file = os.path.join(self._data_dir, 'amyloid_diagnoses_flags.csv')

        self._path_splitdir = os.path.join(dir_split, f'{self.table_name}_split_h5')
        self._path_cleandir_c = os.path.join(dir_split, f'{self.table_name}_clean_h5')

        self._odds_directory = ''
        self._token_dict = ''
        self._model_dir = ''
        self._lsivecs_path = ''
        if odds_ratio:
            self._odds_directory = get_odds_dir(odds_ratio, name=name)
            if os.path.exists(dir_raw) and not os.path.exists(self._odds_directory):
                os.mkdir(self._odds_directory)
            self._token_dict = os.path.join(self._odds_directory, f'{self.table_name}_token_dict{self._name}.txt')

        if num_topics:
            self._model_dir = get_model_dir(odds_ratio, num_topics, name=name)
            if os.path.exists(dir_raw) and not os.path.exists(self._model_dir):
                os.mkdir(self._model_dir)
            self._lsivecs_path = os.path.join(self._model_dir, f'lsi_vecs')

        self._counts_directory = os.path.join(dir_raw, f'word_counts{self._name}')
        if os.path.exists(dir_raw) and not os.path.exists(self._counts_directory):
            os.mkdir(self._counts_directory)

        self._path_cnts = os.path.join(self._counts_directory, f'{self.table_name}_token_counts{self._name}')
        if os.path.exists(dir_raw) and not os.path.exists(self._path_cnts):
            os.mkdir(self._path_cnts)


        self._bow_path = os.path.join(self._odds_directory, BOW)
        if os.path.exists(dir_raw) and not os.path.exists(self._bow_path):
            os.mkdir(self._bow_path)

        self._lsi_vecs = None
        if self._lsivecs_path:
            if os.path.exists(dir_raw) and not os.path.exists(self._lsivecs_path):
                os.mkdir(self._lsivecs_path)

        self._lda_vecs = None
        self._ldavecs_path = os.path.join(self._model_dir, f'lda_vecs')
        if os.path.exists(dir_raw) and not os.path.exists(self._ldavecs_path):
            os.mkdir(self._ldavecs_path)

        self.bowfile = os.path.join(self._bow_path, f'{self.table_name}_bow.h5')
        self.h5bow = H5ColStore(self.bowfile)
        self.lsivecsfile = os.path.join(self._lsivecs_path, f'{self.table_name}_lsi_vecs.h5')
        self.h5lsivecs = H5ColStore(self.lsivecsfile)
        self.ldavecsfile = os.path.join(self._ldavecs_path, f'{self.table_name}_lda_vecs.h5')
        self.h5ldavecs = H5ColStore(self.ldavecsfile)

        # cache variables
        self._pat_index = None
        self._pat_lookup = None
        self._num_patients = None
        self._num_raw_rows = None

        self._lsi_vecs = 'lsi_vecs'
        self._lda_vecs = 'lda_vecs'
        self._num_topics = num_topics
        self._odds_ratio = odds_ratio
        self._dictionary_cache = None

    def iter_tfidf(self, training_only=False):
        # wrapper around iter bag of words

        dictionary = self.read_dictionary()
        tfidf_model = gensim.models.TfidfModel(dictionary=dictionary)

        # yield the tfidf model output of bag of words
        for i, (pat_id, bow) in enumerate(self.iter_bow(training_only=training_only)):
            yield pat_id, tfidf_model[bow]

    def iter_bow(self, training_only=False):

        data = self.h5bow.read_ctable(self.table_name)
        for i, pat_id in enumerate(data[self.id_col]):
            if training_only and self._is_test_group(data[GROUP][i]):
                continue
            if self._restrict_patients and pat_id not in self._restrict_patients:
                continue
            yield pat_id, data[BOW][i]

    @staticmethod
    def _log_time(i, sttime, nrows):
        log_time(i, sttime, nrows)

    def _get_date(self, date_str: str) -> str:
        try:
            month, day, year = date_str.split('/')
            dt = datetime(year=int(year), month=int(month), day=int(day))
            dt = dt.strftime('%Y-%m-%d')
        except:
            dt = ''
        return dt

    def _get_pat_group(self, patient_id: str, num_groups: int=0) -> int:
        # return 1 of self._num_pat_groups integer based on patient id.  Patients randomly split (roughly) evenly
        if not num_groups:
            num_groups = self.num_groups
        return int(hashlib.sha256(patient_id.encode('utf-8')).hexdigest(), 16) % num_groups

    def pat_group_string(self, patient_id: str, num_groups: int=0):
        if not num_groups:
            num_groups = self.num_groups

        g = self._get_pat_group(patient_id, num_groups)
        return self._group_to_string(g, num_groups)

    def _group_to_string(self, g: int, num_groups: int=0):
        if not num_groups:
            num_groups = self.num_groups
        if num_groups < 100:
            return f'g{g:02}'
        elif num_groups < 1000:
            return f'g{g:03}'
        elif num_groups < 10000:
            return f'g{g:04}'
        elif num_groups < 100000:
            return f'g{g:05}'
        else:
            raise NotImplementedError

    def read_e85_file_flags(self) -> dict:
        return self._read_binary_csv_generic(self._flag_file)

    def dictionary_exists(self):
        return os.path.exists(self._token_dict)

    def token_counts_exist(self):
        return os.path.exists(self._token_dict)

    def bow_exists(self):
        return os.path.exists(self.bowfile)

    def lsivecs_exist(self):
        return os.path.exists(self.lsivecsfile)

    def ldavecs_exist(self):
        return os.path.exists(self.ldavecsfile)

    def read_dictionary(self):
        # return the dictionary saved by gensim
        if not self._dictionary_cache:
            self._dictionary_cache = gensim.corpora.Dictionary.load_from_text(self._token_dict)
        return self._dictionary_cache

    def get_amyloid_pat_ids(self) -> set:
        if self._amyloid_pats:
            return self._amyloid_pats

        amyloid_pats = set(open(os.path.join(self.dir_out, 'amyloidosis_patients_icd9_icd10.txt')).read().strip().split('\n'))
        if self._restrict_patients:
            amyloid_pats.intersection_update(set(self._restrict_patients))
        return amyloid_pats

    def _all_split_files(self):
        return glob.glob(os.path.join(self._path_splitdir, f'*.h5'))

    def _all_clean_files(self):
        return glob.glob(os.path.join(self._path_cleandir_c, f'*.h5'))

    def _get_filename_group(self, filename: str):
        basename = os.path.basename(filename)
        group = basename.replace('.h5', '')
        return group

    def _store_key_value(self, key: str, value: Any, h5file=None):
        # simple key value store for easier access to complex stored variables

        if h5file is None:
            h5file = self.h5feat

        assert value is not None
        if not os.path.exists(h5file._h5file) or not h5file.table_info(key):
            h5file.append_ctable(key, col_data={'key': [key], 'value': [value]})
        else:
            h5file.update_ctable(key, query=['key', '==', key], col_data={'key': [key], 'value': [value]})

    def _is_test_group(self, group: str) -> bool:
        # is the group a test group
        gnum = int(group.replace('g', ''))
        if gnum >= 8000:
            return True
        return False

    def _get_key_value(self, key: str, h5file=None):

        if h5file is None:
            h5file = self.h5feat

        # simple key value store access for easier access to complex stored variables
        if not os.path.exists(h5file._h5file) or not h5file.table_info(key):
            return None
        logging.info(f'Reading {key} ...')
        sttime = time.time()
        value = h5file.read_ctable(key, cols=['value'])['value'][0]
        logging.info(f'   time to read {self.table_name}:{key} time: {time.time()-sttime:0.4f} seconds')
        return value

    def _delete_key_value(self, key: str, h5file=None):
        if h5file is None:
            h5file = self.h5feat
        if not os.path.exists(h5file._h5file) or not h5file.table_info(key):
            return None
        h5file.delete_ctable(key, raise_exception=False)

    def _read_binary_csv_generic(self, filename: str) -> dict:

        data = {}
        with open(filename) as f:
            csvreader = csv.reader(f)
            for i, row in enumerate(csvreader):
                if i == 0:
                    data = {x: [] for x in row}
                    continue

                for j, k in enumerate(data.keys()):
                    if j == 0:
                        data[k].append(row[j])
                    else:
                        data[k].append(int(row[j]))

        return data

    def iter_split_rows(self, cols=None, query=(), as_dict: bool=True):

        if cols:
            assert self.id_col in cols

        for h5filepath in self._all_split_files():
            group = os.path.basename(h5filepath).replace('.h5', '')

            h5file = H5ColStore(h5filepath)
            data = h5file.read_ctable(group, cols=cols, query=query)
            if not data:
                continue

            for i, pat_id in enumerate(data[self.id_col]):
                if as_dict:
                    return_row = {k: data[k][i] for k in data}
                else:
                    return_row = [data[k][i] for k in data]

                yield return_row

    def _generate_tokens(self, text: str, min_len: int=3, max_len: int=64, kmer: int=1) -> str:
        # iterator to generate tokens from text
        if kmer > 2:
            raise NotImplementedError

        last_token = ''
        for token in is_space.split(text.lower().strip()):

            token_len = len(token)
            if token_len < min_len or token_len > max_len or is_web.search(token):
                continue

            # substitute all non-alpha numeric
            token = is_not_a_z.sub(' ', token)
            for subtoken in is_space.split(token):
                subtoken = subtoken.strip()
                if len(subtoken) < min_len:
                    continue

                if kmer == 2:
                    if last_token:
                        yield f'{last_token}_{subtoken}'
                    last_token = subtoken
                else:
                    yield subtoken

    def eliminate_duplicate_text(self, text: str, cut_len: int=30):

        exists = {}
        keep = [True for _ in range(len(text))]
        i = 0
        while i < len(text):
            substr = text[i:i+cut_len]

            if substr not in exists:
                exists[substr] = [i]
            else:
                if i >= exists[substr][-1] + cut_len:
                    for j in range(i, i+cut_len):
                        keep[j] = False
            i += 1

        new_text = []
        for i, should_keep in enumerate(keep):
            if should_keep:
                new_text.append(text[i])
            else:
                new_text.append(' ')

        # keep return characters
        ret_text = re.sub('\t+', ' ', ''.join(new_text))
        ret_text = re.sub(' +', ' ', ret_text)
        return ret_text

    def _get_text_sort_col(self):

        text_col = self._file_def[TEXT_COL]
        sort_col = self._file_def[SORT_COL]
        if isinstance(text_col, str):
            text_col = [text_col]

        cols = [self.id_col]
        cols.extend(text_col)
        cols.append(sort_col)

    def get_patient_document(self, get_pat_id: str):

        text_col = self._file_def[TEXT_COL]
        sort_col = self._file_def[SORT_COL]
        if isinstance(text_col, str):
            text_col = [text_col]

        cols = [self.id_col]
        cols.extend(text_col)
        cols.append(sort_col)

        group = self.pat_group_string(get_pat_id)
        h5filepath = os.path.join(self._path_splitdir, f'{group}.h5')
        h5file = H5ColStore(h5filepath)
        group = self._get_filename_group(h5filepath)
        # pull all data at same time
        data = h5file.read_ctable(group, cols=cols, query=[self.id_col, '==', get_pat_id])

        # convert data into numpy arrays
        data[self.id_col] = np.array(data[self.id_col])
        data[sort_col] = np.array(data[sort_col])

        document = []
        for pat_id in sorted(set(data[self.id_col])):
            if pat_id != get_pat_id:
                continue
            inds = np.nonzero(data[self.id_col] == pat_id)[0]
            sorted_subinds = np.argsort(data[sort_col][inds])

            # concatenate all documents and clean them
            for subind in sorted_subinds:
                for col in text_col:
                    document.append(data[col][inds[subind]])

        document = '\n'.join(document)
        clean = self.eliminate_duplicate_text(document)

        data = {'raw': document, 'clean': clean}
        return data

    def create_clean_text_split(self):

        text_col = self._file_def[TEXT_COL]
        sort_col = self._file_def[SORT_COL]
        if isinstance(text_col, str):
            text_col = [text_col]

        cols = [self.id_col]
        cols.extend(text_col)
        cols.append(sort_col)

        sttime = time.time()
        all_files = self._all_split_files()
        for i, h5filepath in enumerate(all_files):

            h5file = H5ColStore(h5filepath)
            group = self._get_filename_group(h5filepath)
            # pull all data at same time
            data = h5file.read_ctable(group, cols=cols)

            # convert data into numpy arrays
            data[self.id_col] = np.array(data[self.id_col])
            data[sort_col] = np.array(data[sort_col])

            col_data = {self.id_col: [], 'text': []}
            for pat_id in sorted(set(data[self.id_col])):
                inds = np.nonzero(data[self.id_col] == pat_id)[0]
                sorted_subinds = np.argsort(data[sort_col][inds])

                # concatenate all documents and clean them
                document = []
                for subind in sorted_subinds:
                    for col in text_col:
                        document.append(data[col][inds[subind]])

                document = '\n'.join(document)

                document = ' '.join([t for t in self._generate_tokens(document)])
                document = self.eliminate_duplicate_text(document)

                col_data[self.id_col].append(pat_id)
                col_data['text'].append(document)

            if not os.path.exists(self._path_cleandir_c):
                os.mkdir(self._path_cleandir_c)
            h5clean = H5ColStore(os.path.join(self._path_cleandir_c, f'{group}.h5'))
            h5clean.append_ctable(group, col_data)
            self._log_time(i, sttime, len(all_files))

    def _iter_h5_files(self, file_paths: list, training_only: bool=True) -> Tuple[H5ColStore, str]:

        for i, h5filepath in enumerate(file_paths):

            group = self._get_filename_group(h5filepath)

            if training_only and self._is_test_group(group):
                # ensure training/learning is only done on training group
                continue

            h5file = H5ColStore(h5filepath)
            yield (h5file, group)

    def create_token_counts(self):
        """Perform token counts of the cleaned text for later dictionary and filtering"""

        all_files = self._all_clean_files()
        amyloid_pats = self.get_amyloid_pat_ids()

        sttime = time.time()
        other_cnts = {}
        other_occurence = {}
        amyloid_cnts = {}
        amyloid_occurence = {}

        for i, (h5file, group) in enumerate(self._iter_h5_files(all_files, training_only=True)):

            data = h5file.read_ctable(group)

            for j, pat_id in enumerate(data[self.id_col]):
                if self._restrict_patients and pat_id not in self._restrict_patients:
                    continue
                document = data['text'][j]

                is_amy = False
                if pat_id in amyloid_pats:
                    is_amy = True

                doc_counts = Counter(self._generate_tokens(document))
                for token, cnt in doc_counts.items():
                    if is_amy:
                        amyloid_occurence[token] = amyloid_occurence.get(token, 0) + 1
                        amyloid_cnts[token] = amyloid_cnts.get(token, 0) + cnt
                    else:
                        other_occurence[token] = other_occurence.get(token, 0) + 1
                        other_cnts[token] = other_cnts.get(token, 0) + cnt

            log_time(i, sttime, len(all_files))

        self._write_cnts_file(f'{self.table_name}_other_cnts.txt', other_cnts)
        self._write_cnts_file(f'{self.table_name}_other_occurence.txt', other_occurence)
        self._write_cnts_file(f'{self.table_name}_amyloid_cnts.txt', amyloid_cnts)
        self._write_cnts_file(f'{self.table_name}_amyloid_occurence.txt', amyloid_occurence)

    def _write_cnts_file(self, file_name: str, cnt_dict: dict):

        if not os.path.exists(self._path_cnts):
            os.mkdir(self._path_cnts)

        with open(os.path.join(self._path_cnts, file_name), 'w', newline='') as f:
            for word, cnt in sorted(cnt_dict.items(), key=lambda item: item[1], reverse=True):
                f.write(f'{word}\t{cnt}\n')

    def _read_cnts_file(self, file_name: str):

        cnts = {}
        with open(os.path.join(self._path_cnts, file_name), 'r') as f:
            for line in f:
                (word, cnt) = line.strip().split('\t')
                cnts[word] = int(cnt)

        return cnts

    def read_patient_clean(self, pat_id: str):

        group = self.pat_group_string(pat_id)
        h5filename = os.path.join(self._path_cleandir_c, f'{group}.h5')
        h5file = H5ColStore(h5filename)
        data = h5file.read_ctable(group, query=(self.id_col, '==', pat_id))
        return data

    def training_patient_counts(self):
        # count all patients in data and return how many (amyloid_cnts, other_cnts) there are.

        amyloid_pats = self.get_amyloid_pat_ids()
        all_files = self._all_clean_files()
        amyloid_cnts = 0
        other_cnts = 0
        for i, (h5file, group) in enumerate(self._iter_h5_files(all_files, training_only=True)):
            pat_ids = h5file.read_ctable(group, cols=[self.id_col])[self.id_col]
            for pat_id in pat_ids:
                if self._restrict_patients and pat_id not in self._restrict_patients:
                    continue
                if pat_id in amyloid_pats:
                    amyloid_cnts += 1
                else:
                    other_cnts += 1

        return amyloid_cnts, other_cnts

    def create_token_dictionary(self,
                                amy_exclude_frac=0.85,
                                other_exclude_frac=0.6,
                                min_amy_cnt = 10,
                                min_other_cnt = 50,
                                top_n_amy=10000,
                                total_words=50000,
                                ):

        other_cnts = self._read_cnts_file(f'{self.table_name}_other_cnts.txt')
        other_occurence = self._read_cnts_file(f'{self.table_name}_other_occurence.txt')
        amyloid_cnts = self._read_cnts_file(f'{self.table_name}_amyloid_cnts.txt')
        amyloid_occurence = self._read_cnts_file(f'{self.table_name}_amyloid_occurence.txt')

        (num_amyloid, num_others) = self.training_patient_counts()
        logging.info(f'Patient counts: amyloid-{num_amyloid}, other-{num_others}')

        exclude_tokens = set()

        # exclude words that are in >x% of amyloid patients or not in other(i.e. exclusive to amyloid)
        exclude_threshold = amy_exclude_frac * num_amyloid
        for token, cnt in amyloid_occurence.items():
            if cnt > exclude_threshold or token not in other_occurence:
                exclude_tokens.add(token)

        # exclude very common words that are in x% of max count of other patients
        common_exclude_threshold = other_exclude_frac * num_others
        for token, cnt in other_occurence.items():
            if cnt > common_exclude_threshold:
                exclude_tokens.add(token)

        # using max_other to estimate total number of other for not present
        # exclude words that are some likelihood ratio above threshold
        # compute odds ratio for each word: (#amyloid_present / #amyloid_not_present) / (#other_present / #other_not_present)
        odds_ratios = {}
        for token, amyloiod_present_cnt in amyloid_occurence.items():

            other_present_cnt = other_occurence.get(token, 0)
            if token in \
                    exclude_tokens or \
                    not other_present_cnt or \
                    (num_amyloid == amyloiod_present_cnt) or \
                    (num_others == other_present_cnt):

                exclude_tokens.add(token)
                continue

            amyloid_ratio = (amyloiod_present_cnt / (num_amyloid - amyloiod_present_cnt))
            other_ratio = (other_present_cnt / (num_others - other_present_cnt))
            odds_ratios[token] = amyloid_ratio / other_ratio

        self._write_cnts_file(f'{self.table_name}_odds_ratios.txt', odds_ratios)
        # exclude words by odds ratio, if > or < odds ratio more likely
        for token, oratio in odds_ratios.items():
            if oratio >= self._odds_ratio or oratio <= (1/self._odds_ratio):
                exclude_tokens.add(token)

        # create gensim dictionary
        dictionary = gensim.corpora.Dictionary()

        # build dictionary, keeping first 10k of amyloid absolutely
        dict_cnt = 0
        for token, amyloid_cnt in amyloid_cnts.items():
            other_cnt = other_cnts.get(token, 0)
            if token in exclude_tokens or amyloid_cnt < min_amy_cnt or other_cnt < min_other_cnt:
                continue

            # add the total number of words as if all documents were read and counted
            dictionary.add_documents([[token] for _ in range(amyloid_cnt + other_cnt)])
            dict_cnt += 1
            if dict_cnt >= top_n_amy:
                break

        for token, other_cnt in other_cnts.items():
            if token in exclude_tokens or token in amyloid_cnts or other_cnt < min_other_cnt:
                continue
            dictionary.add_documents([[token] for _ in range(other_cnt)])
            dict_cnt += 1
            if dict_cnt >= total_words:
                break

        dictionary.compactify()
        dictionary.save_as_text(self._token_dict)

    def create_bow(self):

        all_files = self._all_clean_files()
        dictionary = self.read_dictionary()
        sttime = time.time()
        bow_col_data = {self.id_col: [], GROUP: [], BOW: []}
        for i, (h5file, group) in enumerate(self._iter_h5_files(all_files, training_only=False)):

            data = h5file.read_ctable(group)

            for j, pat_id in enumerate(data[self.id_col]):
                if self._restrict_patients and pat_id not in self._restrict_patients:
                    continue
                document = data['text'][j]

                token_doc = list(self._generate_tokens(document))
                bow_col_data[self.id_col].append(pat_id)
                bow_col_data[GROUP].append(group)
                bow_col_data[BOW].append(dictionary.doc2bow(token_doc))

            log_time(i, sttime, len(all_files))

        col_dtypes = {
            self.id_col: f's{len(bow_col_data[self.id_col][0])}',
            BOW: obj_dtype(bow_col_data[BOW]),
            GROUP: f's{len(bow_col_data[GROUP][0])}',
        }
        self.h5bow.delete_ctable(self.table_name)
        self.h5bow.append_ctable(self.table_name, col_data=bow_col_data, col_dtypes=col_dtypes)

    def lsi_vecs_data(self):

        lsi_vecs = self._get_key_value(self._lsi_vecs, h5file=self.h5vecs)

        data = {self.id_col: []}
        for pat_id, vecs in lsi_vecs.items():
            data[self.id_col].append(pat_id)

            if not vecs:
                # todo: figure out why some vectors do not exist
                for topic in range(self._num_topics):
                    data[f'{self.name}_topic_{topic}'].append(0.0)

                # raise Exception(f'Patient {pat_id} without lsi vecs: {vecs}')
            else:
                for topic, weight in vecs:
                    topicname = f'{self.name}_topic_{topic}'
                    if topicname not in data:
                        data[topicname] = []
                    data[topicname].append(float(weight))

        logging.info(f'Loaded Lsi vecs: {len(data[self.id_col])} and {len(data[topicname])}')

        return data

    def get_demographics_vecotors(self):

        logging.info("Reading demographics dataframes...")

        sttime = time.time()

        AGE = 'CURR_AGE_YRS'
        GENDER = 'GENDER_NM'
        ETH = 'NIH_ETHNCTY'
        RACE = 'NIH_RACE'
        cols = [
            self.id_col,
            AGE,
            GENDER,
            ETH,
            RACE
        ]

        all_files = self._all_split_files()
        data = {c: [] for c in cols}
        for i, h5filepath in enumerate(all_files):
            h5file = H5ColStore(h5filepath)
            group = self._get_filename_group(h5filepath)
            # pull all data at same time
            subdata = h5file.read_ctable(group, cols=cols)
            for c in subdata:
                data[c].extend(subdata[c])

        logging.info(f'Time to load demographics: {time.time()-sttime:0.3f} seconds')

        gends = set()
        eths = set()
        races = set()
        for i, pat_id in enumerate(data[self.id_col]):
            gends.add(simple_clean(data[GENDER][i]))
            eths.add(simple_clean(data[ETH][i]))
            races.add(simple_clean(data[RACE][i]))

        gends = sorted(gends)
        eths = sorted(eths)
        races = sorted(races)

        org_data = {'patients': {}, 'columns': [AGE]}

        # add all one-hot encoded columns
        gend_cols = [f'{GENDER}_{x}' for x in gends]
        eth_cols = [f'{ETH}_{x}' for x in eths]
        race_cols = [f'{RACE}_{x}' for x in races]
        org_data['columns'].extend(gend_cols)
        org_data['columns'].extend(eth_cols)
        org_data['columns'].extend(race_cols)

        for i, pat_id in enumerate(data[self.id_col]):
            age = int(data[AGE][i])
            org_data['patients'][pat_id] = [age]

            # specify column names active for patient
            gen_col = f'{GENDER}_{simple_clean(data[GENDER][i])}'
            eth_col = f'{ETH}_{simple_clean(data[ETH][i])}'
            race_col = f'{RACE}_{simple_clean(data[RACE][i])}'
            has_columns = {gen_col, eth_col, race_col}

            for col in org_data['columns'][1:]:
                if col in has_columns:
                    org_data['patients'][pat_id].append(1)
                else:
                    org_data['patients'][pat_id].append(0)

        logging.info(f'Total time to read demographics: {time.time()-sttime:0.3f} seconds')

        return org_data

    def get_model_vectors(self, vec_type='lsi'):
        """
        Return a dictionary of 'patients' and 'columns'.  Where columns lists the column names (should be globally unique)
        for a row of data for each patient.   'patients' is a dictionary of patient_id to row vector.
        """

        if self.table_name == 'Demographics':
            return self.get_demographics_vecotors()

        lda_nlp_vecs = {}
        isboth = False
        if vec_type == 'lsilda':
            nlp_vecs = self._get_key_value(VECS, self.h5lsivecs)
            lda_nlp_vecs = self._get_key_value(VECS, self.h5ldavecs)
            isboth = True
        elif vec_type == 'lsi':
            nlp_vecs = self._get_key_value(VECS, self.h5lsivecs)
        elif vec_type == 'lda':
            nlp_vecs = self._get_key_value(VECS, self.h5ldavecs)
        else:
            raise Exception(f'Need correct vec_type: {vec_type}')

        data = {'patients': {}, 'columns': []}
        # init lsi_vecs structure
        col_num = 0
        no_vecs = []
        sub_num_topics = 0
        for j, (pat_id, vecs) in enumerate(nlp_vecs.items()):

            row = []

            if not vecs:
                no_vecs.append(pat_id)
                # todo: figure out why some vectors do not exist
                continue

            if vecs and len(vecs) != self._num_topics:
                sub_num_topics += 1

            lda_vecs = {}
            if isboth:
                lda_vecs = lda_nlp_vecs.get(pat_id, None)
                if not lda_vecs:
                    no_vecs.append(pat_id)
                    continue

            weight_dict = dict(vecs)
            lda_w_dict = dict(lda_vecs)
            for topic in range(self._num_topics):

                row.append(float(weight_dict.get(topic, 0)))
                if isboth:
                    row.append(float(lda_w_dict.get(topic, 0)))

                if j == 0:
                    # initialize columns
                    if isboth:
                        colname = f'{self.table_name}_lsi_{topic}'
                    else:
                        colname = f'{self.table_name}_{vec_type}_{topic}'

                    data['columns'].append(colname)
                    col_num += 1

                    if isboth:
                        colname = f'{self.table_name}_lda_{topic}'
                        data['columns'].append(colname)
                        col_num += 1

            data['patients'][pat_id] = row

        logging.info(f"No vecs number={len(no_vecs)}")
        logging.info(f'Sub {self._num_topics} number={sub_num_topics}')
        logging.info(f'Loaded columng vecs {self.table_name}: num columns={col_num} num patients{len(data["patients"])}')

        return data

    def create_lda_vecs(self):

        logging.info(f'Starting training LDA for {self.table_name}')
        sttime = time.time()
        gensim_model = gensim.models.LdaModel
        dictionary = self.read_dictionary()

        corpus = [tfidf for pat_id, tfidf in self.iter_tfidf(training_only=True)]
        model = gensim_model(corpus, id2word=dictionary,
                             num_topics=self._num_topics,
                             passes=40,
                             random_state=1,  # for reproducibility
                             alpha='symmetric',
                             #eta='auto',
                             chunksize=10000,
                             iterations=100,
                             decay=0.5,  # a little higher forget rate
                             offset=16,  # 256 seemed high from paper
                             dtype=np.float64,
                             )

        print("MODEL INFORMATION...")
        print(model)

        logging.info(f'Saving model...')
        model_name = f'{self.table_name}_LDA'
        model.save(os.path.join(self._ldavecs_path, f'{model_name}.model'))
        try:
            # write out a topics file to review
            model.print_topics(num_topics=20, num_words=20)

            with open(os.path.join(self._ldavecs_path, f'{model_name}_topics.txt'), 'w', newline='') as f:
                for top_id, vals in model.print_topics(num_topics=-1, num_words=100):
                    f.write(f'{top_id}\t{vals}\n')
        except:
            logging.error(f'Model could not print_topics')

        logging.info(f'Training time for LDA for {self.table_name}: {time.time()-sttime:0.2f} seconds')
        logging.info(f'Generating LDA vectors for entire data set...')
        # generate model vectors for entire data set including test
        data = {pat_id: model[tfidf] for (pat_id, tfidf) in self.iter_tfidf(training_only=False)}
        self._store_key_value(VECS, data, self.h5ldavecs)

    def create_lsi_vecs(self):

        logging.info(f'Starting training LSI for {self.table_name}')
        sttime = time.time()
        gensim_model = gensim.models.LsiModel
        dictionary = self.read_dictionary()

        corpus = [tfidf for pat_id, tfidf in self.iter_tfidf(training_only=True)]
        model = gensim_model(corpus, id2word=dictionary,
                             num_topics=self._num_topics,
                             chunksize=20000,
                             onepass=False,
                             power_iters=4,
                             dtype=np.float64,
                             )

        print("MODEL INFORMATION...")
        print(model)

        logging.info(f'Saving model...')
        model_name = f'{self.table_name}_LSI'
        model.save(os.path.join(self._lsivecs_path, f'{model_name}.model'))
        try:
            # write out a topics file to review
            model.print_topics(num_topics=20, num_words=20)

            with open(os.path.join(self._lsivecs_path, f'{model_name}_topics.txt'), 'w', newline='') as f:
                for top_id, vals in model.print_topics(num_topics=-1, num_words=100):
                    f.write(f'{top_id}\t{vals}\n')
        except:
            logging.error(f'Model could not print_topics')

        logging.info(f'Training time for LSI for {self.table_name}: {time.time()-sttime:0.2f} seconds')
        logging.info(f'Generating LSI vectors for entire data set...')
        # generate model vectors for entire data set including test
        data = {pat_id: model[tfidf] for (pat_id, tfidf) in self.iter_tfidf(training_only=False)}
        self._store_key_value(VECS, data, self.h5lsivecs)

    # todo: potentially deprecate
    def _iter_groups(self):

        # iterate through the split file group names
        table_names = []
        for group_num in range(self.num_groups):
            table_name = self._group_to_string(group_num, num_groups=self.num_groups)
            table_names.append(table_name)

        logging.info(f'Iterating all grouped note tables: {len(table_names)} total')
        for table_name in table_names:
            yield table_name

    def iter_raw_rows(self, block_size=4000) -> dict:
        """
        Iterate through every row of data returning a dictionary of column names to data for each row.

        :param block_size: determines how many rows to read simultaneously (block reads are faster but increase memory)
        :return:
        """

        nrows = self.h5raw.table_nrows(self.table_name)
        for i in range(0, nrows, block_size):
            lastrow = min(i+block_size, nrows-1)
            data = self.h5raw.read_ctable(self.table_name, inds=list(range(i, lastrow)))
            for i, pat_id in enumerate(data[self.id_col]):
                yield {k: data[k][i] for k in data}

    def pat_id_data(self, pat_id: str, log_on: bool=False, cols=None) -> dict:
        """Read all information for a single patient id"""

        group = self.pat_group_string(pat_id)

        query = (self.id_col, '==', pat_id)
        sttime = time.time()
        data = self._read_table_h5splitnew(group, cols=cols, query=query, log_on=log_on)
        if log_on:
            logging.info(f'Time to read {self.table_name} for pat_id={pat_id} {time.time()-sttime:0.3f} seconds')

        return data

    def _read_table_h5splitnew(self, table_name: str, cols=None, query=(), log_on: bool=False) -> dict:

        if self._single_file:
            return self._read_table(self.h5splitnew, table_name, cols=cols, query=query, log_on=log_on)
        else:
            h5file = H5ColStore(os.path.join(self._path_splitdir, f'{table_name}.h5'))
            return self._read_table(h5file, table_name, cols=cols, query=query, log_on=log_on)

    def _read_table(self, h5file: H5ColStore, table_name: str, cols=None, query=(), log_on: bool=False) -> dict:
        # iterate through all patient notes in a single table created as a split table
        try:
            data = h5file.read_ctable(table_name, cols=cols, query=query)
        except:
            if not h5file.table_info(table_name):
                # can happen randomly since patient ids are hashed, but should be rare
                if log_on:
                    logging.warning(f'No table information for {table_name}')
                data = {}
            else:
                raise

        return data

    def _create_index(self, force=False):
        """
        Perform patient indexing
        :return:
        """

        if not force and self.h5feat.table_info(PAT_INDEX):
            logging.info(f'Index {self.table_name}:{PAT_INDEX} exists, not creating')
            return

        logging.info(f'Create index {self.table_name}:{PAT_INDEX} ...')
        pat_ids = []
        pat_index = {}
        sttime = time.time()
        for i, pat_id in enumerate(self.h5raw.read_ctable(self.table_name, cols=[self.id_col])[self.id_col]):
            if pat_id not in pat_index:
                pat_index[pat_id] = []
                pat_ids.append(pat_id)
            pat_index[pat_id].append(i)

        pat_lookup = dict(zip(list(pat_ids), range(len(pat_ids))))
        self._store_key_value(PAT_INDEX, pat_index)
        self._store_key_value(PAT_LOOKUP, pat_lookup)
        self._store_key_value(NUM_PATIENTS, len(pat_lookup))
        logging.info(f'Create index {self.table_name}:{PAT_INDEX} time: {time.time()-sttime:0.4f} seconds')

    def num_patients(self):
        if not self._num_patients:
            self._num_patients = self._get_key_value(NUM_PATIENTS)
        return self._num_patients

    def patient_ids(self):
        # return a list of patient ids
        return list(self.pat_lookup())

    def get_pat_indices(self, pat_id: str):
        """
        Return all indicies of the patient in the intial raw file.
        Patient index.

        Return None if doesn't exist.

        :param pat_id:
        :return:
        """
        return self.pat_index().get(pat_id, None)

    def get_pat_row(self, pat_id: str) -> Optional[int]:
        """
        Return the row position in any aggregated patient table.

        Return None if doesn't exist.

        :param pat_id:
        :return:
        """
        return self.pat_lookup().get(pat_id, None)

    def pat_lookup(self) -> dict:
        # return the feature patient lookup dictionary, pat_id -> index in feature
        if not self._pat_lookup:
            self._pat_lookup = self._get_key_value(PAT_LOOKUP)
        return self._pat_lookup

    def pat_index(self) -> dict:
        # return the raw patient index dictionary, pat_id -> [list of row index in raw]
        if not self._pat_index:
            self._pat_index = self._get_key_value(PAT_INDEX)
        return self._pat_index