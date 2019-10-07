#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "October 2019"


from datetime import datetime,timedelta


def singleNight(today,dateobs,logfile):
    try:
        s_int = datetime.strptime(today, "%Y-%m-%dT%H:%M:%S.%f")
        e_int = s_int + timedelta(1)
        start = datetime.strftime(s_int, '%Y-%m-%dT%H:%M:%S.%f')
        end = datetime.strftime(e_int, '%Y-%m-%dT%H:%M:%S.%f')
        if end >= dateobs >= start:	
	    return True
        else:
	    return False
    except Exception as e:
	logfile.write('%s -- Exception: %s \n' % (datetime.now(),e))
