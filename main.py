#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "June 2018"

from read_json import ReadJson
from utilities import VerifyLinux
from utilities import LoggingClass

from database import MySQLDatabase
from database import LUCITable
from database import Queries
from database import Queries2


log = LoggingClass('',True).get_logger()

def main():
    try:
        VerifyLinux()
        rj = ReadJson()

        dbuser = rj.get_db_user()
        dbpwd  = rj.get_db_pwd()
        dbname = rj.get_db_name()
        dbhost = rj.get_db_host()
        dbport = rj.get_db_port()

        db      = MySQLDatabase(dbuser, dbpwd, dbname, dbhost, dbport)
        Session = db.mysql_session()

        filename = 'luci.20140904.0002.fits.gz'
        file_version = '0'

        #rows = Queries(Session, LUCITable, filename).match_filename()
        #if not rows:
        #    print(filename)

        rows2 = Queries2(Session, LUCITable, filename, file_version).get_storage_path()
        #if not rows2:
        #    print(filename, file_version)

    except Exception as e:
        msg = "Main exception - main() -- "
        log.error("{0}{1}".format(msg,e))

if __name__ == "__main__":
   main()
