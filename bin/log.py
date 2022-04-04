# coding=UTF-8

import os
import logging
import logging.handlers

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

DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR  = logging.ERROR
CRITICAL = logging.CRITICAL
