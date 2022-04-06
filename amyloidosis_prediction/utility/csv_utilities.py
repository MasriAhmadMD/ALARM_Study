"""
Code to split large files into sub-files of grouped patients.
"""

import csv
import io
import logging
import glob
import os
import re

# the text fields may be raeher large so need to increase field limit size
from amyloidosis_prediction.data_objects.file_config import NAME, ENCODING, QUOTECHAR, DELIM, ID_COL, COL_DEF, EXPECTED_ROWS

csv.field_size_limit(2147483647)


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