#!/usr/bin/env python

__author__ = "Elisa Londero"
__email__ = "elisa.londero@inaf.it"
__date__ = "January 2020"

import os
import sys
import smtplib
import logging
import subprocess
import email.utils
import logging.handlers
from os.path import isdir
from os.path import isfile
from astropy.io import fits
from email.mime.text import MIMEText

class VerifyLinux(object):
    assert ('linux' in sys.platform), "Function can only run on Linux systems."

class LoggingClass(object):
    def __init__(self, logger_name='root', create_file=False):
        self.logger_name = logger_name
        self.create_file = create_file

    def get_logger(self):
        log = logging.getLogger(self.logger_name)
        log.setLevel(level=logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s','%Y-%m-%d %H:%M:%S')

        if self.create_file:
            fh = logging.FileHandler('file.log')
            fh.setLevel(level=logging.DEBUG)
            fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setLevel(level=logging.DEBUG)
        ch.setFormatter(formatter)

        if self.create_file:
            log.addHandler(fh)

        log.addHandler(ch)
        return  log

class MissingConfParameter(Exception):
    def __init__(self, par):
        #super().__init__(f"Parameter {par} not defined in configuration file")
        self.par = par

log = LoggingClass('',True).get_logger()

class SendEmail(object):
    def __init__(self, message, recipient, sender, smtphost):
        self.message = message
        self.recipient = recipient
        self.sender = sender
        self.smtphost = smtphost

    def send_email(self):
        hostname = self.sender.split("@",1)[1].title() 
        msg = MIMEText(self.message)
        msg['To'] = email.utils.formataddr(('To', self.recipient))
        msg['From'] = email.utils.formataddr((hostname+'WatchDog', self.sender))
        msg['Subject'] = hostname + ' alert'
        try:
            server = smtplib.SMTP(self.smtphost, 25)
        except Exception as e:
            msg = "SMTP connection excep - SendEmail.send_email -- " 
            log.error("{0}{1}".format(msg,e))
        else:
            server.sendmail(self.sender, [self.recipient], msg.as_string())
            server.quit()

class ReadNigths(object):
    def __init__(self, filename):
        self.filename = filename

    def _get_nights_list(self):
        try:
            filepath = '%s/%s' % (os.getcwd(), self.filename)
            stations_list = []
            with open(filepath, 'r') as filehandle:
                for line in filehandle:
                    currentPlace = line[:-1]
                    stations_list.append(currentPlace)
            return stations_list
        except Exception as e:
            msg = "Read nights excep - ReadNights._get_nights_list -- "
            log.error("{0}{1}".format(msg,e))

class md5Checksum(object):
    def __init__(self, filename):
        self.filename = filename

    def calculate_checksum(self):
        try:
            hash_md5 = hashlib.md5()
            if self.filename is None:
                pass
            else:
                with open(self.filename, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                return hash_md5.hexdigest()
        except Exception as e:
            msg = "Checksum calculation excep - md5Checksum.calculate_checksum -- "
            log.error("{0}{1}".format(msg,e))

    def get_checksum_gz(self):
        try:
            fopen = gzip.open(self.filename, 'rb')
            fcontent = fopen.read()
            chks = hashlib.md5(fcontent).hexdigest()
            fopen.close()
            return chks
        except Exception as e:
            msg = "Checksum calculation excep - md5Checksum.get_checksum_gz -- "
            log.error("{0}{1}".format(msg,e))
