#!/usr/bin/env python3

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


'''
   This script sorts out the calibration files based on the DATE
   contained in the input file and distributes them towards a
   number of destinations.
   Usage: python calibDistributor.py
   Requires: python3
'''


import sys
import os
import pymysql
from astropy.io import fits
from singleNight import singleNight
from md5Checksum import md5Checksum
from readTools import readCSV,readJson
from datetime import date,time,datetime,timedelta
from transferTools import sftpTransfer,scpTransfer
from mysqlTools import mysqlConnection,mysqlInsert,storagePathConstructor


CWD = os.path.dirname(os.path.abspath(sys.argv[0]))
logfile = open(CWD + '/' + "logfile.txt",'a')
cnf = readJson('conf.json',CWD,logfile)
datelist = readCSV(cnf['dates_file'],logfile)
sshkey = cnf['priv_sshkey']
db_check = cnf['db_schema_check']
tbl_check = cnf['db_table_check']

db1 = pymysql.connect(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema'])
cur1 = db1.cursor()
db2 = pymysql.connect(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema_check'])
cur2 = db2.cursor()

cksm_query = "SELECT checksum,destination FROM " + tbl_check + " WHERE filename=%s;"

for i in cnf['instruments']:
    query = i.get('query'); table = i.get('table')

    try:
        cur1.execute(query, [datelist[0][0]])
    except pymysql.Error as e:
        logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

    for x in cur1.fetchall():
        for j in datelist:
            date_parser = singleNight(j[0],x[6],logfile)
            if (date_parser):
                version = x[1]
                storage_path = storagePathConstructor(cur1,version,table,x[0],logfile)
                localpath = storage_path + x[0]
                cksm_storage = md5Checksum(localpath,logfile)

                try:
                    cur2.execute(cksm_query, [x[0]])
                    referenceDB = cur2.fetchall()
                except pymysql.Error as e:
                    logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

                cksmDB_list = []
                destDB_list = []

                for k in i.get('destination'):
                        host = k.get('host'); user = k.get('user')
                        label = k.get('label'); mode = k.get('mode')
                        fold = k.get('fold')
                        remotepath = fold + x[0]

			# this loops over the destinations for each instrument and transfers new files not found yet in DB
                        if not referenceDB:
                            if mode == 'scp':
                                returncode = scpTransfer(host,user,sshkey,localpath,remotepath,logfile)
                            elif mode == 'sftp':
                                returncode = sftpTransfer(host,user,sshkey,localpath,remotepath,logfile)
                            if (returncode):
                                mysqlInsert(db_check,tbl_check,cur2,db2,x[0],localpath,x[2],x[3],x[4],x[5],x[6],x[7],label,cksm_storage,logfile)

                        # this part takes care of new versions of files and files not transferred previously because of transfer issues
                        elif (referenceDB):
                            for l in range(len(referenceDB)):
                                cksmDB_list.append(referenceDB[l][0])
                                destDB_list.append(referenceDB[l][1])

                            if ((cksm_storage in cksmDB_list and label not in destDB_list) or (cksm_storage not in cksmDB_list)):
                                if mode == 'scp':
                                    returncode = scpTransfer(host,user,sshkey,localpath,remotepath,logfile)
                                elif mode == 'sftp':
                                    returncode = sftpTransfer(host,user,sshkey,localpath,remotepath,logfile)
                                if (returncode):
                                    mysqlInsert(db_check,tbl_check,cur2,db2,x[0],localpath,x[2],x[3],x[4],x[5],x[6],x[7],label,cksm_storage,logfile)

cur1.close()
cur2.close()
logfile.close()
