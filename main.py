#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "June 2018"

from read_json import ReadJson
from utilities import VerifyLinux
from utilities import LoggingClass
from utilities import ReadNights
from utilities import SingleNight

from database import MySQLDatabase
from database import LUCITable
from database import MODSTable
from database import LBCTable
from database import InsertTable
from database import MatchFilenameQuery
from database import StoragePathQuery
from database import CalibrationSelectionQueries
from database import Queries4

# select all calib files from a datetime till today
# select only the calib files from above that belong to inaf nights
#

log = LoggingClass('',True).get_logger()

def main():
    try:
        VerifyLinux()
        rj = ReadJson()

        dbuser = rj.get_db_user()
        dbpwd  = rj.get_db_pwd()
        dbhost = rj.get_db_host()
        dbport = rj.get_db_port()

        dbname_nadir = rj.get_db_name_read()
        dbname_write_record = rj.get_db_name_write()

        db_nadir = MySQLDatabase(dbuser, dbpwd, dbname_nadir, dbhost, dbport)
        Session_nadir  = db_nadir.mysql_session()

        db_write_record = MySQLDatabase(dbuser, dbpwd, dbname_write_record, dbhost, dbport)
        Session_write_record = db_write_record.mysql_session()

        nights_file = rj.get_inaf_nights()
        inaf_nights = ReadNights(nights_file).get_nights_list()

        start_dates_list, end_dates_list = SingleNight(inaf_nights).get_single_night()

        for i in range(len(start_dates_list)):
            luci_calib_filter = CalibrationSelectionQueries(Session_nadir, LUCITable, start_dates_list[i], end_dates_list[i]).luci_query()
            for j in luci_calib_filter:
                print(i,j.file_name,j.date_obs,j.obstype)

    except Exception as e:
        msg = "Main exception - main() -- "
        log.error("{0}{1}".format(msg,e))

if __name__ == "__main__":
   main()
