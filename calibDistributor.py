#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


'''
   This script is supposed to be used for sorting out the
   calibration files based on the DATE displayed on the schedule. 
'''


import sys
import os
import MySQLdb
from astropy.io import fits
from sftpRemote import sftpRemote
from singleNight import singleNight
from scpRemote import scpRemote
from md5Checksum import md5Checksum
from readTools import readCSV,readJson
from datetime import date,time,datetime,timedelta
from mysqlTools import mysqlConnection,mysqlInsert,storagePathConstructor


CWD = os.path.dirname(os.path.abspath(sys.argv[0]))

logfile = open(CWD + '/' + "logfile.txt",'a')

cnf = readJson('config.json',CWD,logfile)

datelist = readCSV(cnf['dates_file'],logfile)

cur1, db1 = mysqlConnection(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema'],logfile)
cur2, db2 = mysqlConnection(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema_check'],logfile)

# LUCI - Roma e Milano
luci_query = "SELECT file_name,file_version,instrument,propid,piname,partner,date_obs,object FROM luci WHERE obstype='CALIBRATION' or obstype='DARK';"

try:
    cur1.execute(luci_query)
except MySQLdb.Error, e:
    logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

for x in cur1.fetchall():
    for i in datelist:
	totrans = singleNight(i[0],x[6],logfile)
	if (totrans):
            version = x[1]
            storage_path = storagePathConstructor(cur1,version,'luci',x[0],logfile)
            filepath = storage_path + x[0]
            cksm_storage = md5Checksum(filepath,logfile)
            print filepath

            cksm_query = "SELECT checksum FROM " + cnf['db_table_check'] + " WHERE filename=%s;"
	    try:
                cur2.execute(cksm_query, [x[0]])
	        cksm_referenceDB = cur2.fetchall()
	    except MySQLdb.Error, e:
	        logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

	    if ((not cksm_referenceDB) or (cksm_referenceDB[0][0] != cksm_storage)):
                # Milano	
		path = cnf['remote_path1_luci'] + x[0]        
                scpRemote(cnf['remote_host1'],cnf['remote_user1'],cnf['priv_sshkey'],filepath,path,logfile)
	        mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],'MI',cksm_storage,logfile)
		# Roma
	        path = cnf['remote_path2'] + x[0]          
	        errcode = sftpRemote(cnf['remote_host2'],cnf['remote_user2'],cnf['priv_sshkey'],filepath,path,logfile)
	        if (errcode):
                    mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],'RM',cksm_storage,logfile)

# LBC - solo Roma
lbc_query = "SELECT file_name,file_version,instrument,propid,piname,partner,date_obs,object FROM lbc WHERE obs_type != 'object';"

try:
    cur1.execute(lbc_query)
except MySQLdb.Error, e:
    logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

for x in cur1.fetchall():
    for i in datelist:
	totrans = singleNight(i[0],x[6],logfile)
	if (totrans):
            version = x[1]
            storage_path = storagePathConstructor(cur1,version,'lbc',x[0],logfile)
            filepath = storage_path + x[0]
            cksm_storage = md5Checksum(filepath,logfile)
            print filepath

            cksm_query = "SELECT checksum FROM " + cnf['db_table_check'] + " WHERE filename=%s;"
            try:
                cur2.execute(cksm_query, [x[0]])
                cksm_referenceDB = cur2.fetchall()
            except MySQLdb.Error, e:
                logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

            if ((not cksm_referenceDB) or (cksm_referenceDB[0][0] != cksm_storage)):
                # Roma
                path = cnf['remote_path2'] + x[0]
                errcode = sftpRemote(cnf['remote_host2'],cnf['remote_user2'],cnf['priv_sshkey'],filepath,path,logfile)
                if (errcode):
                    mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],'RM',cksm_storage,logfile)

# MODS - solo Milano
mods_query = "SELECT file_name,file_version,instrument,propid,piname,partner,date_obs,object FROM mods WHERE imagetyp != 'OBJECT'"

try:
    cur1.execute(mods_query)
except MySQLdb.Error, e:
    logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

for x in cur1.fetchall():
    for i in datelist:
	totrans = singleNight(i[0],x[6],logfile)
	if (totrans):
            version = x[1]
            storage_path = storagePathConstructor(cur1,version,'mods',x[0],logfile)
            filepath = storage_path + x[0]
            cksm_storage = md5Checksum(filepath,logfile)
            print filepath

            cksm_query = "SELECT checksum FROM " + cnf['db_table_check'] + " WHERE filename=%s;"
            try:
                cur2.execute(cksm_query, [x[0]])
                cksm_referenceDB = cur2.fetchall()
            except MySQLdb.Error, e:
                logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

            if ((not cksm_referenceDB) or (cksm_referenceDB[0][0] != cksm_storage)):
                # Milano    
                path = cnf['remote_path1_mods'] + x[0]
                scpRemote(cnf['remote_host1'],cnf['remote_user1'],cnf['priv_sshkey'],filepath,path,logfile)
	        mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],'MI',cksm_storage,logfile)
			
logfile.close()			
