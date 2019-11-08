#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "August 2019"


'''
    Function md5Checksum calculates the checksum of the
    file given in input.
'''

import hashlib
from datetime import datetime


def md5Checksum(fname,logfile):
    hash_md5 = hashlib.md5()
    try:
        with open(fname,"rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except IOError as e:
        logfile.write('%s -- IOError: %s \n' % (datetime.now(),e))
        exit(1)
