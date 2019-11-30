#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


"""
    Function sftpTransfer is used to transfer files towards
    a destination that allows an ssh session to be started.
    The size of the transferred file is checked and compared
    to the file on the localhost. The success/failure of the
    transfer is based on that comparison. The connection to
    remote machine is based on ssh keys.
    Function scpTransfer is used to transfer files towards a
    destination that does not allow an ssh connection (eg
    when restricted-ssh is used). In this case there is no
    check on transfer success/failure.
"""

import os
import paramiko
import subprocess
from datetime import datetime


def sftpTransfer(server,username,sshkey,localpath,remotepath,logfile):
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(server,username=username,key_filename=sshkey)
        sftp = client.open_sftp()
        sftp.put(localpath,remotepath)
        stat_remote = sftp.stat(remotepath)
        sftp.close()
        client.close()
        stat_local = os.stat(localpath)
        if stat_local.st_size == stat_remote.st_size:
            return True
        else:
            return False
    except Exception as e:
        logfile.write('%s -- SFTP Exception: %s \n' % (datetime.now(),e))


def scpTransfer(hostname,username,sshkey,localpath,remotepath,logfile):
    try:
        cmd = "scp -i " + sshkey + " " + localpath + " " + username + "@" + hostname + ":" + remotepath
        exit_code = subprocess.call(cmd,shell=True)
        if exit_code == 0:
            return True
        else:
            return False
    except subprocess.CalledProcessError as e:
        logfile.write('%s -- subprocess.CalledProcessError: %s \n' % (datetime.now(),e))
