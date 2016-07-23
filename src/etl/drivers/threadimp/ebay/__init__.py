from etl.extractors.ebay import EBayExtractor
from etl.tranformers.ebay import EBayTransformers
from etl.loaders.dumps import DumpLoader, get_dump_location
import cloud
from etl.config import logger
import os
from etl.config.drivers import THREADEDEBAY
#from etl.loaders.sqlite.sqlitecloud import SQLlitePiCloudloader



class ThreadedEBayETLDriver(object):
    
    def __init__(self, *args, **kwargs):
        self.extractor = EBayExtractor()
        self.transformer = EBayTransformers()
        self.loader = DumpLoader()
        self.customlogger = THREADEDEBAY
        #self.loader=SQLlitePiCloudloader()
        object.__init__(self, *args, **kwargs)

