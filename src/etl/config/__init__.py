import logging
from logging import handlers
from logging.handlers import RotatingFileHandler
import os
import stat
import json
import sys

from etl.config.loadconfig import COMPANY_CONFIG
from etl.config.service import service
from etl.config.drivers import THREADEDEBAY



TRIALSELLERDURATION = 30

DUMPS_TIMELIMIT_TO_DELETE_IN_DAYS=5

BESTMATCHONLY = "BESTMATCHONLY"
OVERVIEWTHRESHOLD = COMPANY_CONFIG['account-overview-threshold']
ETL_DEBUG_PRINT = True
PRINT_PROGRESS = True
ETLDUMPLOCATION = str(COMPANY_CONFIG['etl']['dumps'])

print "DUMP PATH IS: %s" % ETLDUMPLOCATION
if not os.path.exists(ETLDUMPLOCATION):
    print "ETL PATH DOEST NOT EXIST"
    #sys.exit()

COMPANY_ITEM_SERVICE_ACCOUNT_OVERVIEW = COMPANY_CONFIG['sis']['deployment']['source'] + "/src/company_items/etl/useroverviewupdate.py"

#config = ConfigParser.RawConfigParser()
#config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),'config.cfg'))




class config(object):
    _instance = None
    
    def __new__(self, *args, **kwargs):
        if self._instance == None:
            self.ENVIRONMENT = kwargs['environment']
            self.DRIVERTYPE = kwargs.get('drivertype',THREADEDEBAY)
            servebase = service[self.ENVIRONMENT]
            self.SOLR_BASE_URL = servebase['SOLR_BASE_URL']
            self.SERVICE_BASE = servebase['SERVICE_BASE']
            self.ROOT_KEY = servebase['ROOT_KEY']
            self.SCORING_SERVICE_BASE = servebase['SCORING_SERVICE_BASE']
            self.METADATA_SERVICE_BASE = servebase['METADATA_SERVICE_BASE']
        return self
    


class GiveWriteRotatingFileHandler(handlers.RotatingFileHandler):

    def doRollover(self):
        """
        Override base class method to make the new log file group writable.
        """
        # Rotate the file first.
        handlers.RotatingFileHandler.doRollover(self)

        # Add group write to the current permissions.
        currMode = os.stat(self.baseFilename).st_mode
        os.chmod(self.baseFilename, currMode | stat.S_IRWXO)
        
formatter = logging.Formatter('%(asctime)s %(name)-15s %(levelname)-15s %(module)-15s %(funcName)-15s %(lineno)-15s %(message)s')
logging.basicConfig()

logger = logging.getLogger("SERVICE")
logger.setLevel(logging.NOTSET)


#console = logging.StreamHandler()
#console.setFormatter(formatter)
#console.setLevel(logging.WARNING)
#logger.addHandler(console)

try:
    rfhandler = GiveWriteRotatingFileHandler(filename=COMPANY_CONFIG['etl']['logs']['service'], mode='a', maxBytes=500000 , backupCount=100)
    rfhandler.setFormatter(formatter)
    rfhandler.setLevel(logging.NOTSET)
    logger.addHandler(rfhandler)
except Exception, e:
    print e

alllocale = {'AT':'16', 'AU':'15', 'CH':'193', 'DE':'77', 'ENCA':'2', 'ES':'186', 'FR':'71', 'FRBE':'23', 'FRCA':'210', 'GB':'3', 'HK':'201', 'IE':'205', 'IN':'203', 'IT':'101', 'MOTOR':'100', 'MY':'207', 'NL':'146', 'NLBE':'123', 'PH':'211', 'PL':'212', 'SG':'216', 'US':'0'}
