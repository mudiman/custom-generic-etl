import etl
import cloud
from etl.config.drivers.picloud import config
from etl.drivers.picloud.ebay import PiCloudEBayETLDriver
from etl.loaders.dumps import DumpLoader, get_dump_location
import os
import pickle

class TestPiCloudETLDriver(PiCloudEBayETLDriver):
    
    def __init__(self, *args, **kwargs):
        PiCloudEBayETLDriver.__init__(self, *args, **kwargs)
        self.loader = DumpLoader()
        


def loadfile(filename):
    dumplocation = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(dumplocation, filename)
    f = open(path, 'r')
    data = f.read()
    data = pickle.loads(data)
    f.close()
    return data




sellerid = 'officeshoes'
cloud.setkey(config['keyid'], config['key'])
