import glob
import logging
import os
import time

from simpleh5 import H5ColStore

from amyloidosis_prediction.data_objects.base_obj_common import BaseObjCommon
from amyloidosis_prediction.data_objects.file_config import NAME, COL_DEF, ID_COL
from amyloidosis_prediction.utility.csv_utilities import read_all_csv_lines
from amyloidosis_prediction.utility.timing import log_time
from amyloidosis_prediction.utility.csv_utilities import iterate_csv_rows, write_csv_lines


def split_csvs(input_directory, output_directory, file_definition, limit_rows=0):

    split_dir = os.path.join(output_directory, f'{file_definition[NAME]}_split_csv')
    if not os.path.exists(split_dir):
        os.mkdir(split_dir)
    else:
        raise Exception(f'{split_dir} exists, delete if you want to run')

    bobj = BaseObjCommon(file_definition, create_index=False)
    col_def = file_definition[COL_DEF]
    lkup = dict(zip(list(col_def.keys()), range(len(col_def))))
    buffer = {}
    pat_ids = []
    for cnt, row in enumerate(iterate_csv_rows(input_directory, file_definition, limit_rows=limit_rows, as_dict=False)):
        pat_id = row[lkup[bobj.id_col]]
        pat_ids.append(pat_id)
        group = bobj.pat_group_string(pat_id)

        if group not in buffer:
            buffer[group] = []
        buffer[group].append(row)

        push_len = 10  # write 10 notes at a time for each group
        if len(buffer[group]) > push_len:
            write_csv_lines(os.path.join(split_dir, f'{group}.csv'), buffer[group])
            del buffer[group]
    return pat_ids


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
        log_time(cnt, sttime, len(all_files), pre=f'{table_name} {group}')

    return pat_ids