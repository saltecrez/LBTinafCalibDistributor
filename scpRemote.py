#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "September 2019"


import subprocess 
from datetime import datetime


def scpRemote(server,username,sshkey,localpath,remotepath,logfile):
    try:
	cmd = "scp -i " + sshkey + " " + localpath + " " + username + "@" + server + ":" + remotepath
	subprocess.call(cmd,shell=True)
    except subprocess.CalledProcessError as e:
        logfile.write('%s -- subprocess.CalledProcessError: %s \n' % (datetime.now(),e))
