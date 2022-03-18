"""
Script to characterize clinical note dates on all patients
"""

import os
import time
from datetime import datetime


CURDIR = os.path.dirname(os.path.abspath(__file__))
SORTED_FILE = 'sorted_clinical_note_dates.txt'

def reorganize_dates():
    with open(os.path.join(CURDIR, 'clinical_note_dates.txt'), 'r') as f:
        data = f.read().strip().split('\n')

    sttime = time.time()
    pat_data = {}
    for line in data:
        row = line.split(',')
        pat = row[0]
        dates = []
        for dt in row[1:]:
            month, day, year = dt.split('/')
            newdt = f'{year}-{int(month):02}-{int(day):02}'
            dates.append(newdt)
        pat_data[pat] = dates

    with open(os.path.join(CURDIR, SORTED_FILE), 'w') as f:
        for pat, dates in sorted(pat_data.items()):
            f.write(f'{pat},{",".join(sorted(dates))}\n')

    print(time.time() -sttime)


def characterize_and_return_dates(print_stats=True):

    sttime = time.time()
    if not os.path.exists(SORTED_FILE):
        reorganize_dates()

    with open(os.path.join(CURDIR, SORTED_FILE), 'r') as f:
        data = f.read().strip().split('\n')

    TOTAL = 'total'
    MIN_LESS_2010 = 'min_dt_2010_or_less'
    MAX_GREATER_2010 = 'max_dt_2010_or_more'
    MIN_3 = 'three_or_more_dates'
    MIN_180_DAYS = 'min_180_days_of_data'
    pat_info = {
        TOTAL: set(),
        MIN_LESS_2010: set(),
        MAX_GREATER_2010: set(),
        MIN_3: set(),
        MIN_180_DAYS: set(),
    }
    for line in data:
        row = line.split(',')
        pat = row[0]
        dates = row[1:]
        min_dt = datetime.strptime(dates[0], '%Y-%m-%d')
        max_dt = datetime.strptime(dates[-1], '%Y-%m-%d')
        days = (max_dt-min_dt).days

        pat_info[TOTAL].add(pat)
        if dates[0] < '2010-01-01':
            pat_info[MIN_LESS_2010].add(pat)
        if dates[-1] > '2009-01-01':
            pat_info[MAX_GREATER_2010].add(pat)
        if len(dates) >= 3:
            pat_info[MIN_3].add(pat)
        if days >= 180:
            pat_info[MIN_180_DAYS].add(pat)

    if print_stats:
        with open(os.path.join(CURDIR, 'date_statistics.txt'), 'w') as f:
            for i, (key1, vals1) in enumerate(pat_info.items()):
                f.write(f'{key1},{len(vals1)}\n')
            for i, (key1, vals1) in enumerate(pat_info.items()):
                for j, (key2, vals2) in enumerate(pat_info.items()):
                    if j <= i:
                        continue
                    intersect = vals1.intersection(vals2)
                    f.write(f'{key1}-{key2},{len(intersect)}\n')

    pats = pat_info[TOTAL].intersection(pat_info[MIN_3]).intersection(pat_info[MIN_180_DAYS]).intersection(pat_info[MAX_GREATER_2010])
    print(len(pats))
    return pats


if __name__ == '__main__':
    characterize_and_return_dates(print_stats=False)