from etl.extractors.ebay import EBayExtractor
from etl.tranformers.ebay import EBayTransformers
from etl.loaders.dumps import DumpLoader, get_dump_location
import cloud
import os
from etl.config.drivers import PICLOUDEBAY
#from etl.loaders.sqlite.sqlitecloud import SQLlitePiCloudloader


class loader(DumpLoader):
    
    def write(self, listdict, filename):
        try:
            output = self.serialize(listdict)
            cloud.files.putf(output, filename)
        except Exception, e:
            cloud.cloud.cloudLog.exception(str(e))
        finally:
            output.close()


class PiCloudEBayETLDriver(object):
    
    def __init__(self, *args, **kwargs):
        self.extractor = EBayExtractor()
        self.transformer = EBayTransformers()
        self.loader = loader()
        self.customlogger = PICLOUDEBAY
        #self.loader = DumpLoader()
        #self.loader=SQLlitePiCloudloader()
        object.__init__(self, *args, **kwargs)

