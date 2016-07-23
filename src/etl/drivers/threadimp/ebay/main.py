


from etl.miscfunctions import create_list
from etl.drivers.generic import BuildIndexSkeleton
from etl.config import logger

from etl.drivers.threadimp.ebay.build import build_index 
from etl.dal import update_sellers_status








class ThreadedEBayBuildIndex(BuildIndexSkeleton):
    
    def __init__(self, configuration):
        self.configuration = configuration
        
        
    def start_index(self, sellerids, etlconfiguration="ALL"):

        self.sellers = create_list(sellerids)
        self.start_index_from_current_machine(etlconfiguration)
        return self.filenamelist
        
    
    def start_index_from_current_machine(self, etlconfiguration):
        self.filenamelist = []
        for seller in self.sellers:
            try:
                self.filenamelist += build_index(sellerdata=seller, configuration=self.configuration, etlconfiguration=etlconfiguration)
            except Exception, e:
                logger.exception(str(e))
                obj = dict(sellerid=seller.get('sellerid'))
                update_sellers_status([obj], {"phase": "INDEXING", "progress":str(e)})

        return self.filenamelist;


        
if __name__ == "__main__":
    pass
        
    

        
