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

class TransferTools(object):
    def __init__(self, hostname, username, sshkey, localpath, remotepath):
        self.hostname = hostname
        self.username = username
        self.sshkey = sshkey
        self.localpath = localpath
        self.remtepath = remotepath

    def sftp_transfer(self):
        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.connect(self.hostname,username=self.username,key_filename=self.sshkey)
            sftp = client.open_sftp()
            sftp.put(self.localpath,self.remotepath)
            stat_remote = sftp.stat(self.remotepath)
            sftp.close()
            client.close()
            stat_local = os.stat(self.localpath)
            if stat_local.st_size == stat_remote.st_size:
                return True
            else:
                return False
        except Exception as e:
            msg = "SFTP exception - sftp_transfer() -- "
            log.error("{0}{1}".format(msg,e)) 

    def scp_transfer(self):
        try:
            cmd = "scp -i " + self.sshkey + " " + self.localpath + " " + self.username + "@" + self.hostname + ":" + self.remotepath
            exit_code = subprocess.call(cmd,shell=True)
            if exit_code == 0:
                return True
            else:
                return False
        except Exception as e:
            msg = "SCP exception - scp_transfer() -- "
            log.error("{0}{1}".format(msg,e)
