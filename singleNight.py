#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "October 2019"


'''
    This function decides if an observation date falls
    within a certain time range defining a single night.
    A single night includes 24 h, from the midday of the
    day given in input to the midday of the day after.
    Input dates format: year-month-dayThour:min:sec:msec
    (eg 2019-09-26T12:00:00.000)
'''

from datetime import datetime,timedelta


def singleNight(input_date,dateobs,logfile):
    try:
        s_int = datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S.%f")
        e_int = s_int + timedelta(1)
        start = datetime.strftime(s_int, '%Y-%m-%dT%H:%M:%S.%f')
        end = datetime.strftime(e_int, '%Y-%m-%dT%H:%M:%S.%f')
        if end >= dateobs >= start:
            return True
        else:
            return False
    except Exception as e:
        logfile.write('%s -- Exception: %s \n' % (datetime.now(),e))
