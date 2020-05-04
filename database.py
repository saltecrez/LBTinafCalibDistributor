#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"

import os
import pymysql
from sqlalchemy import or_
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from utilities import LoggingClass
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
log = LoggingClass('',True).get_logger()

class MySQLDatabase(object):
    def __init__(self, user, pwd, dbname, host='localhost', port='3306'):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.dbname = dbname

    def _create_session(self):
        sdb = 'mysql+pymysql://%s:%s@%s:%s/%s'%(self.user,self.pwd,self.host,self.port,self.dbname)
        try:
            engine = create_engine(sdb)
            db_session = sessionmaker(bind=engine)
            return db_session()
        except Exception as e:
            msg = "Database session creation excep - MySQLDatabase._create_session -- "
            log.error("{0}{1}".format(msg,e))

    def _validate_session(self):
        try:
            connection = self._create_session().connection()
            return True
        except Exception as e:
            msg = "Database session validation excep - MySQLDatabase._validate_session -- "
            log.error("{0}{1}".format(msg,e))
            return False

    def mysql_session(self):
       Session = self._validate_session()
       if Session:
           return self._create_session() 
       else:
           exit(1)

    def close_session(self):
        try:
            self._create_session().close()
            return True
        except Exception as e: 
            msg = "Database session closing excep - MySQLDatabase.close_session -- "
            log.error("{0}{1}".format(msg,e))
            return False

class LUCITable(Base):
    __tablename__ = 'luci'

    id = Column(Integer, primary_key=True)
    file_name = Column(String(255))
    file_version = Column(Integer)
    storage_path = Column(String(255))
    file_path = Column(String(255))
    instrument = Column(String(255))
    propid = Column(String(255))
    piname = Column(String(255))
    partner = Column(String(255))
    date_obs = Column(String(255))
    obstype = Column(String(255))
    object = Column(String(255))

    def __init__(self, file_name):
        self.file_name = file_name
        self.file_version = file_version
        self.storage_path = storage_path
        self.file_path = file_path
        self.instrument = instrument
        self.propid = propid
        self.piname = piname
        self.partner = partner
        self.date_obs = date_obs
        self.obstype = obstype
        self.object = object

class MODSTable(Base):
    __tablename__ = 'mods'

    id = Column(Integer, primary_key=True)
    file_name = Column(String(255))
    file_version = Column(Integer)
    storage_path = Column(String(255))
    file_path = Column(String(255))
    instrument = Column(String(255))
    propid = Column(String(255))
    piname = Column(String(255))
    partner = Column(String(255))
    date_obs = Column(String(255))
    imagetyp = Column(String(255))
    object = Column(String(255))

    def __init__(self, file_name):
        self.file_name = file_name
        self.file_version = file_version
        self.storage_path = storage_path
        self.file_path = file_path
        self.instrument = instrument
        self.propid = propid
        self.piname = piname
        self.partner = partner
        self.date_obs = date_obs
        self.imagetyp = imagetyp
        self.object = object

class LBCTable(Base):
    __tablename__ = 'lbc'

    id = Column(Integer, primary_key=True)
    file_name = Column(String(255))
    file_version = Column(Integer)
    storage_path = Column(String(255))
    file_path = Column(String(255))
    instrument = Column(String(255))
    propid = Column(String(255))
    piname = Column(String(255))
    partner = Column(String(255))
    date_obs = Column(String(255))
    obs_type = Column(String(255))
    object = Column(String(255))

    def __init__(self, file_name):
        self.file_name = file_name
        self.file_version = file_version
        self.storage_path = storage_path
        self.file_path = file_path
        self.instrument = instrument
        self.propid = propid
        self.piname = piname
        self.partner = partner
        self.date_obs = date_obs
        self.obs_type = obs_type
        self.object = object

class Queries(object):
    def __init__(self, session, table_object, string):
        self.session = session
        self.table_object = table_object
        self.string = string

    def match_filename(self):
        try:
            rows = self.session.query(self.table_object)
            flt = rows.filter(self.table_object.file_name == self.string)
            for j in flt:
                if j.file_name:
                    return True
                else:
                    return False
        except Exception as e:
            msg = "Match filename string excep - Queries.match_filename -- "
            log.error("{0}{1}".format(msg,e))

class Queries2(object):
    def __init__(self, session, table_object, string1, string2):
        self.session = session
        self.table_object = table_object
        self.string1 = string1
        self.string2 = string2

    def get_storage_path(self):
        try:
            rows = self.session.query(self.table_object)
            flt = rows.filter(self.table_object.file_name == self.string1, self.table_object.file_version == self.string2)
            for j in flt:
                strip_path = j.storage_path.rstrip('/')
                file_path = os.path.join(j.file_path,str(j.file_version),j.file_name)
                storage_path = strip_path + file_path 
            return storage_path
        except Exception as e:
            msg = "Find storage path string excep - Queries2.get_storage_path -- "
            log.error("{0}{1}".format(msg,e))

class Queries3(object):
    def __init__(self, session, table_object, string):
        self.session = session
        self.table_object = table_object
        self.string = string

    def luci_query(self):
        try:
            rows = self.session.query(self.table_object)
            flt = rows.filter(self.table_object.date_obs >= self.string, or_(self.table_object.obstype == 'CALIBRATION', self.table_object.obstype == 'DARK'))
            for j in flt:
                print(j.file_name, j.file_version, j.instrument, j.propid, j.piname, j.partner, j.date_obs, j.object)
        except Exception as e:
            msg = "LUCI query excep - Queries3.luci_query -- "
            log.error("{0}{1}".format(msg,e))

    def mods_query(self):
        try:
            rows = self.session.query(self.table_object)
            flt = rows.filter(self.table_object.date_obs >= self.string, self.table_object.imagetyp != 'OBJECT')
            for j in flt:
                print(j.file_name, j.file_version, j.instrument, j.propid, j.piname, j.partner, j.date_obs, j.object)
        except Exception as e:
            msg = "MODS query excep - Queries3.mods_query -- "
            log.error("{0}{1}".format(msg,e))

    def lbc_query(self):
        try:
            rows = self.session.query(self.table_object)
            flt = rows.filter(self.table_object.date_obs >= self.string, self.table_object.obs_type != 'OBJECT')
            for j in flt:
                print(j.file_name, j.file_version, j.instrument, j.propid, j.piname, j.partner, j.date_obs, j.object)
        except Exception as e:
            msg = "LBC query excep - Queries3.lbc_query -- "
            log.error("{0}{1}".format(msg,e))

