#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


import os
import paramiko
from fabric import Connection
from datetime import datetime


def sftpTransfer(server,username,sshkey,localpath,remotepath,logfile):
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(server,username=username,key_filename=sshkey)
        sftp = client.open_sftp()
        sftp.put(localpath,remotepath)
        stat_local = os.stat(localpath)
        stat_remote = sftp.stat(remotepath)
        sftp.close()
        client.close()
        if stat_local.st_size == stat_remote.st_size:
            return True
        else:
            return False
    except Exception as e:
        logfile.write('%s -- Exception: %s \n' % (datetime.now(),e))


def scpTransfer(hostname,username,sshkey,localpath,remotepath,logfile):
    try:
        c = Connection(host = hostname,
                    user = username,
                    connect_kwargs = {"key_filename": sshkey},
		    connect_timeout = 5)
        result = c.put(localpath,remotepath)
        c.close()
        return True
    except Exception as e:
        logfile.write('%s -- Exception: %s \n' % (datetime.now(),e))
        return False
