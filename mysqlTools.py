#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


import pymysql
from datetime import datetime


def mysqlConnection(host,user,password,schema,logfile):
    try:
        db = pymysql.connect(host,user,password,schema)
        cur = db.cursor()
        return cur, db
    except pymysql.Error as e:
        logfile.write('%s -- pymysql.Error: %s \n' % (datetime.now(),e))


def mysqlInsert(schema,table,cur,db,filename,filepath,instrument,propid,piname,partner,date_obs,object,destination,cksm_storage,logfile):
    query = "INSERT INTO " + schema + "." + table + \
            "(filename,filepath,instrument,propid,piname,partner,date_obs,object,destination,checksum)" + \
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    try:
        cur.execute(query,[filename,filepath,instrument,propid,piname, \
                           partner,date_obs,object,destination,cksm_storage])
        db.commit()
    except pymysql.Error as e:
        logfile.write('%s -- pymysql.Error: %s \n' % (datetime.now(),e))


def storagePathConstructor(cur,file_version,dbtable,filename,logfile):
    sql_query = "select storage_path, file_path from " + dbtable + \
                " where file_name=%s and file_version=%s;"
    try:
        cur.execute(sql_query,[filename,file_version])
        result_query = cur.fetchall()
        storage_path = result_query[0][0]
        file_path = result_query[0][1]
        full_path = storage_path + file_path + '/' + str(file_version) + '/'
        return full_path
    except pymysql.Error as e:
        logfile.write('%s -- pymysql.Error: %s \n' % (datetime.now(),e))
