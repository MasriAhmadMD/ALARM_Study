"""
Code to split large files into sub-files of grouped patients.
"""

import csv
import io
import logging
import glob
import os
import re
import time
import traceback

from simpleh5 import H5ColStore

# the text fields may be raeher large so need to increase field limit size
from amyloidosis_prediction.data_objects.file_config import NAME, ENCODING, QUOTECHAR, DELIM, ID_COL, COL_DEF, EXPECTED_ROWS
from amyloidosis_prediction.utility.timing import log_time
from amyloidosis_prediction.data_objects.base_obj_common import BaseObjCommon

csv.field_size_limit(2147483647)


def read_csv_lines(filename: str, encoding='utf-8', delimiter='|', quotechar='"') -> list:
    rows = []
    with open(filename, 'r', encoding=encoding, newline='') as csvfile:
        reader = csv.reader(csvfile, quotechar=quotechar, delimiter=delimiter)
        rows.extend([row for row in reader])
    return rows


def read_all_csv_lines(filename: str, encoding='utf-8', delimiter='|', quotechar='"') -> list:
    rows = []
    with open(filename, 'r', encoding=encoding, newline='') as csvfile:
        data = csvfile.read()

    reader = csv.reader(io.StringIO(data), quotechar=quotechar, delimiter=delimiter)
    rows.extend([row for row in reader])
    return rows


def write_csv_lines(filename: str, rows: list, encoding='utf-8', delimiter='|', quotechar='"'):

    filepath = os.path.dirname(filename)
    if filepath and not os.path.exists(filepath):
        os.mkdir(filepath)
    with open(filename, 'a', encoding=encoding, newline='') as csvfile:
        writer = csv.writer(csvfile, quotechar=quotechar, delimiter=delimiter)
        writer.writerows(rows)


def iterate_csv_rows(in_directory: str, file_definition: dict, limit_rows=10000, as_dict: bool=True):
    """
    Iterate through rows of all files in a directory matching the file match string.
    :param in_directory:
    :param file_definition: dictionary of parameters to read files
    :param limit_rows: limit number of rows in each file read, if 'falsy' then will read whole file
    :return:
    """

    file_match = file_definition['name']
    # get list of all files matching
    file_list = glob.glob(os.path.join(in_directory, f'*{file_match}*.csv'))

    logging.info(f'Reading files in directory: {in_directory}')
    logging.info(f'Found {len(file_list)} files matching: {file_match}')

    for file in file_list:
        logging.info(f'Reading: {file}')

        # since some files have errors before finishing, catch and continue rather than crash
        errorcnt = 0
        num_errors = 0
        with open(file, 'r', encoding=file_definition[ENCODING]) as csvfile:

            cnt = None
            try:
                reader = csv.reader(csvfile, delimiter=file_definition[DELIM], quotechar=file_definition[QUOTECHAR])
                header = None
                for cnt, line in enumerate(reader):
                    if cnt == 0:
                        header = line
                        # hack substitution of wierd character
                        if re.search(file_definition[ID_COL], header[0], re.IGNORECASE):
                            header[0] = file_definition[ID_COL]
                        logging.info(f"HEADER: {header} in {file}")
                        continue

                    if limit_rows and cnt == limit_rows:
                        logging.info(f'Break due to limit_rows: {limit_rows}')
                        break
                    if cnt % 500000 == 0:
                        logging.info(f'  {file_match} at row: {cnt} in {file}')

                    # hardcoded check of what should be valid patient id in first column
                    patient_id = line[0]
                    if len(patient_id) != 32:
                        errorcnt += 1
                        logging.error(f"Bad patient id on line {cnt} number {errorcnt}: {line[0:32]}")
                    # iterator here to return dictionary rows for file
                    if as_dict:
                        yield dict(zip(header, line))
                    else:
                        yield line

            except GeneratorExit:
                pass
            except:
                #logging.error(f'***{cnt} Start Last line***\n{lastline}\n***End last line***\n***Start line***'
                #              f'\n{line}\n***End line***')
                # raise
                num_errors += 1
                logging.error(f'error #{num_errors} occurred on line {cnt}')



def csv_split_to_hdf5_split(in_directory: str, out_directory: str, file_definition: dict):
    '''
    Perform conversion of original csv format to hdf5

    :param in_directory:
    :param out_directory:
    :param file_definition:
    :return:
    '''

    # loop through all different file types
    outfile = os.path.join(out_directory, f'{file_definition[NAME]}_split.h5')
    if os.path.exists(outfile):
        raise Exception(f'Existing: File exists: {outfile}')

    bobj = BaseObjCommon(file_definition)
    h5file = H5ColStore(outfile)
    table_name = file_definition[NAME]
    col_def = file_definition[COL_DEF]

    logging.info(f'Writing files to directory: {out_directory}')
    logging.info(f'Checking file_definition: {file_definition}')

    split_dir = os.path.join(in_directory, f'{file_definition[NAME]}_split_csv')
    all_files = glob.glob(os.path.join(split_dir, '*.csv'))
    sttime = time.time()
    pat_ids = []
    for cnt, fpath in enumerate(all_files):
        max_lens = [0 for _ in col_def]
        col_data = {k: [] for k in col_def}
        for row in read_all_csv_lines(fpath):
            for j, col in enumerate(col_def):
                val = row[j]
                if col == file_definition[ID_COL]:
                    pat_ids.append(val)
                lval = len(val)
                if lval > max_lens[j]:
                    max_lens[j] = lval
                col_data[col].append(val)

        group = os.path.basename(fpath).replace('.csv', '')
        h5splitdir = bobj._path_splitdir
        if not os.path.exists(h5splitdir):
            os.mkdir(h5splitdir)
        h5file = os.path.join(bobj._path_splitdir, f'{group}.h5')
        h5 = H5ColStore(h5file)
        h5.append_ctable(group, col_data, col_dtypes=file_definition[COL_DEF])

        #new_dtypes = {k: f's{max(1, int(1.1*max_lens[j]))}' for j, k in enumerate(col_def)}
        #h5file.create_ctable(group, new_dtypes, expectedrows=file_definition[EXPECTED_ROWS])
        #h5file.append_ctable(group, col_data=col_data)
        log_time(cnt, sttime, len(all_files), pre=f'{table_name} {group}')

    return pat_ids


def csv_to_hdf5(in_directory: str, out_directory: str, file_definition: dict, limit_rows=5000, buffer_len=5000):
    '''
    Perform conversion of original csv format to hdf5

    :param in_directory:
    :param out_directory:
    :param file_definition:
    :param limit_rows:
    :param buffer_len:
    :return:
    '''

    # loop through all different file types
    outfile = os.path.join(out_directory, f'{file_definition[NAME]}.h5')
    if os.path.exists(outfile):
        logging.error(f'Existing: File exists: {outfile}')
        return

    logging.info(f'Writing files to directory: {out_directory}')
    logging.info(f'Checking file_definition: {file_definition}')

    h5file = H5ColStore(outfile)
    table_name = file_definition[NAME]
    col_def = file_definition[COL_DEF]
    h5file.create_ctable(table_name, col_def, expectedrows=file_definition[EXPECTED_ROWS])

    # loop through all files and re-write rows
    logging.info(f'Starting read and writing ...')
    buffer = {k: [] for k in col_def}
    logging.info(f'Columns: {",".join([k for k in col_def])}')
    id_col = list(buffer.keys())[0]
    for cnt, row in enumerate(iterate_csv_rows(in_directory, file_definition, limit_rows=limit_rows)):

        for k in row:
            buffer[k].append(row[k])

        if buffer[id_col] and len(buffer[id_col]) % buffer_len == 0:
            h5file.append_ctable(table_name, col_data=buffer)
            # clear buffer
            buffer = {k: [] for k in col_def}

    # ensure buffer is emptied
    if buffer:
        h5file.append_ctable(table_name, col_data=buffer)


if __name__ == '__main__':
    rows = [['junk1', 'junk2', 'junk3'], ['j1', 'j2', 'j3']]
    f = './junk.csv'
    write_csv_lines(f, rows)
    d1 = read_csv_lines(f)
    print(d1)
    d2 = read_all_csv_lines(f)
    print(d2)
