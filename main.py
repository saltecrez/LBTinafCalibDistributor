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
        first_night = inaf_nights[0]

        luci_calib_filter = CalibrationSelectionQueries(Session_nadir, LUCITable, first_night).luci_query()
        for i in luci_calib_filter:
            for j in inaf_nights:
                tf = SingleNight(j,i.date_obs).get_single_night()
                if tf:
                    print(i.file_name,i.date_obs)

    except Exception as e:
        msg = "Main exception - main() -- "
        log.error("{0}{1}".format(msg,e))

if __name__ == "__main__":
   main()
