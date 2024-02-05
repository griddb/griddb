# coding=UTF-8

import os
import logging
import logging.handlers
import subprocess
from typing import Optional

def get_tz() -> str:
    p1 = subprocess.Popen(['date','+%z'], stdout=subprocess.PIPE)
    tz: Optional[str] = bytesToStr(p1.communicate()[0].split()[0])
    if tz == '+0000':
        return 'Z'
    return tz[0:3] + ':' + tz[3:5]

def get_dt() -> str:
    p2 = subprocess.Popen(['date', '+%FT%T.%3N'], stdout=subprocess.PIPE)
    return bytesToStr(p2.communicate()[0].split()[0])

def get_local_time(*args) -> Optional[str]:
    return get_dt() + get_tz()

def logger(binfile, level):
    logdir = os.environ["GS_LOG"]
    filebase = os.path.basename(binfile)
    logFile = os.path.join(logdir, filebase + ".log")
    log = logging.getLogger()
    handler = logging.handlers.RotatingFileHandler(
        logFile, maxBytes=(5*1024*1024), backupCount=10)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(process)d] [%(levelname)s] %(funcName)s(%(lineno)d) %(message)s'))
    log.addHandler(handler)
    log.setLevel(level)
    return log

def bytesToStr(value) -> Optional[str]:
    rval = value
    vtype = type(value)
    if vtype is bytes:
        rval = value.decode()
    return rval

DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR  = logging.ERROR
CRITICAL = logging.CRITICAL

logging.Formatter.formatTime = get_local_time
