from logging.handlers import RotatingFileHandler
from etl.config import formatter, COMPANY_CONFIG
import logging
import os



def getsanitylogger():
    rfhandler = RotatingFileHandler(filename=COMPANY_CONFIG['etl']['logs']['sanity'], mode='a', backupCount=100)
    rfhandler.setFormatter(formatter)
    rfhandler.setLevel(logging.INFO)
    logging.basicConfig()
    sanitylogger = logging.getLogger("SANITY")
    return sanitylogger
