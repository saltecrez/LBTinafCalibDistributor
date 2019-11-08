#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "June 2018"


'''
   Function readJson is used to read the json configuration file.
   Function readCSV is used to read a csv file.
'''

import json
import csv
from datetime import datetime


def readJson(filename,cwd,logfile):
    json_config_file_path = '%s/%s' % (cwd,filename)
    config_properties = {}
    try:
        with open(json_config_file_path) as data_file:
            config_properties = json.load(data_file)
        return config_properties
    except IOError as e:
        logfile.write('%s -- IOError: %s \n' % (datetime.now(),e))
        exit(1)


def readCSV(filename,logfile):
    rowlist = []
    try:
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file,delimiter=',')
            for row in csv_reader:
                rowlist.append(row)
        return rowlist
    except IOError as e:
        logfile.write('%s -- IOError: %s \n' % (datetime.now(),e))
        exit(1)
