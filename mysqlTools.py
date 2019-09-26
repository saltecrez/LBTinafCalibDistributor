#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"

import MySQLdb
from datetime import datetime

def mysqlConnection(host,user,password,schema,logfile):
    try:
        db = MySQLdb.connect(host,user,password,schema)
        cur = db.cursor()
    	return cur, db
    except MySQLdb.Error, e:
        logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))


def mysqlInsert(schema,table,cur,db,filename,filepath,cksm_storage,propid,destination,logfile):
    query = "INSERT INTO " + schema + "." + table + "(filename,filepath,checksum,propid,destination) VALUES(%s,%s,%s,%s,%s);"
    try:
        cur.execute(query,[filename,filepath,cksm_storage,propid,destination])
        db.commit()
    except MySQLdb.Error, e:
        logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))


def storagePathConstructor(cur,file_version,dbtable,filename,logfile):
    sql_query = 'select storage_path, file_path from ' + dbtable + ' where file_name=%s and file_version=%s;'
    try:
        cur.execute(sql_query, [filename,file_version])
        result_query = cur.fetchall()
        storage_path = result_query[0][0]
        file_path = result_query[0][1]
        full_path = storage_path + file_path + '/' + str(file_version) + '/'
        return full_path
    except MySQLdb.Error, e:
        logfile.write('%s -- MySQLdb.Error: %s \n' % (datetime.now(),e))
