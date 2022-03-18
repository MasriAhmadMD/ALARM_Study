"""utility for timing"""
import logging

import time


def log_time(i, sttime, nrows, pre=''):

    time_per_line = None
    curtime = time.time() - sttime
    if i and i > 100000 and i % 100000 == 0:
        time_per_line = curtime / i
    elif i and i < 100000 and i % 10000 == 0:
        time_per_line = curtime / i
    elif i and i < 10000 and i % 1000 == 0:
        time_per_line = curtime / i
    elif i and i < 1000 and i % 100 == 0:
        time_per_line = curtime / i
    elif i and i < 100 and i % 10 == 0:
        time_per_line = curtime / i
    if time_per_line is not None:
        logging.info(f'{pre} line {i} of {nrows} est time left: {(nrows-i)*time_per_line:0.4f} seconds current run time: {curtime}')
