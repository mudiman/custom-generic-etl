'''
Created on Oct 21, 2011

@author: Mudassar
'''

from abc import abstractmethod




class BuildIndexSkeleton():
    
    @abstractmethod
    def start_index(self, sellerids):
        raise NotImplementedError()


#class ETLDriver(object):
#    
#    def __new__(self, *args, **kwargs):
#        from etl.drivers.picloud.ebay import PiCloudEBayETLDriver
#        
#        driver=kwargs.get('driver',None)
#        if not driver:
#            raise Exception("No driver pass")
#        if driver=="etl.drivers.picloud.ebay":
#            return PiCloudEBayETLDriver()
#        raise Exception("No driver Available")
        
    
    
