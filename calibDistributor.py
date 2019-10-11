#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


'''
   This script sorts out the calibration files based on the DATE 
   contained in the schedules and distributes them towards the 
   required destinations.
   Usage: python2.7 calibDistributor.py 
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

cnf = readJson('conf.json',CWD,logfile)

datelist = readCSV(cnf['dates_file'],logfile)

cur1, db1 = mysqlConnection(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema'],logfile)
cur2, db2 = mysqlConnection(cnf['db_host'],cnf['db_user'],cnf['db_pwd'],cnf['db_schema_check'],logfile)

cksm_query = "SELECT checksum,destination FROM " + cnf['db_table_check'] + " WHERE filename=%s;"

for i in cnf['destinations']:
    query = i.get('query'); table = i.get('table')
    dest  = i.get('dest');  host  = i.get('host')
    user  = i.get('user');  label = i.get('label')
    mode = i.get('mode')

    try:
	cur1.execute(query, [datelist[0][0]])
    except MySQLdb.Error, e:
	logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

    for x in cur1.fetchall():
	for j in datelist:
	    date_parser = singleNight(j[0],x[6],logfile)
	    if (date_parser):
		version = x[1]
		storage_path = storagePathConstructor(cur1,version,table,x[0],logfile)
		filepath = storage_path + x[0]
		cksm_storage = md5Checksum(filepath,logfile)

		try:
		    cur2.execute(cksm_query, [x[0]])
		    referenceDB = cur2.fetchall()
		except MySQLdb.Error, e:
		    logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))

		cksmDB = referenceDB[0][0]; destDB = referenceDB[0][1];
		if ((not cksmDB) or (cksmDB != cksm_storage) or (cksmDB == cksm_storage and label != destDB)):		
		    path = dest + x[0]
		    if mode == 'scp':
			print label,x[0],mode
			scpRemote(host,user,cnf['priv_sshkey'],filepath,path,logfile) 
			mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],label,cksm_storage,logfile)
		    elif mode == 'sftp':
			print label,x[0],mode
			errcode = sftpRemote(host,user,cnf['priv_sshkey'],filepath,path,logfile)
			if (errcode):
			    mysqlInsert(cnf['db_schema_check'],cnf['db_table_check'],cur2,db2,x[0],filepath,x[2],x[3],x[4],x[5],x[6],x[7],label,cksm_storage,logfile)

logfile.close()			
