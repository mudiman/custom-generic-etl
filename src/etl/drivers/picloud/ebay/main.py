import cloud
import os


from etl.miscfunctions import create_list
from etl.drivers.generic import BuildIndexSkeleton
from etl.config.drivers.picloud import config, setcloudkey, setcloudloglevel,\
    removelockfile
from etl.config import logger
from etl.loaders.dumps import get_dump_location
from etl.drivers.picloud.ebay.build import build_index , \
    build_bestmatchonly_index

#from etl.dal import update_sellers_status, get_sellers_items
#from etl.loaders.sqlite.sqlitecloud import SQLlitePiCloudloader

from etl.dal import update_sellers_status
from etl.config.drivers.picloud.ebay import SELLERITEMDETAIL_ALLOWED_JOB_FAIL_COUNT
import time






class PiCloudEBayBuildIndex(BuildIndexSkeleton):
    
    def __init__(self, configuration):
        self.configuration = configuration
        
        
    def start_index(self, sellerids, etlconfiguration="ALL"):
       
        setcloudkey()
        setcloudloglevel()

        self.sellers = create_list(sellerids)
        
        
        filenamelist = self.start_indexing_on_cloud(etlconfiguration)
        #filenamelist=self.start_index_from_current_machine(etlconfiguration)

        return filenamelist

        
    
    def start_indexing_on_cloud(self, etlconfiguration):
        try:
            successfullsellerfilenamelist=[]
            jobids = []
            self.filenamelist = []
            self.successsellers = []
            mainjobids = []
            for seller in self.sellers:
                try:
                    update_sellers_status([seller], dict(phase="INDEXING", progress="STARTED"))
                    if etlconfiguration == "BESTMATCHONLY":
                        tempjobids = build_bestmatchonly_index(sellerdata=seller, configuration=self.configuration, etlconfiguration=etlconfiguration)
                        seller['itemdetailjobid'] = tempjobids
                        mainjobids.append(tempjobids)
                    else:
                        jobids.append(cloud.call(build_index, sellerdata=seller, configuration=self.configuration, etlconfiguration=etlconfiguration, _label="SELLER"))
                        seller['jobid'] = jobids[len(jobids) - 1]
                    self.successsellers.append(seller)
                except Exception, e:
                    logger.exception(str(e))
                    update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
                finally:
                    pass
            print "waiting for main seller job to complete"
            logger.debug("waiting for main seller job to complete")
            cloud.join(jobids, ignore_errors=True)

            
            if etlconfiguration != "BESTMATCHONLY":
                for seller in self.successsellers:
                    if cloud.status(seller['jobid']) in ('error', 'stalled', 'killed'):
                        update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
                        seller['jobcompletecheck'] = True
                        seller['itemdetailjobid'] = None
                        logger.exception(seller['sellerid'] + " job failed at initial stage")
                    else:
                        seller['itemdetailjobid'] = cloud.result(seller['jobid'], ignore_errors=False)
                        mainjobids.append(cloud.result(seller['jobid'], ignore_errors=False))
            
            print "waiting for seller cloud jobids list"
            logger.debug("waiting for seller cloud jobids list")
            cloud.join(mainjobids, ignore_errors=True)
            filenames = []
            itemdetailjobids = []
            for seller in self.successsellers:
                try:
                    if seller.get('itemdetailjobid'):
                        if cloud.status(seller['itemdetailjobid']) in ('error', 'stalled', 'killed'):
                            update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
                            seller['jobcompletecheck'] = True
                            logger.exception(seller['sellerid'] + " job failed before fetch item details")
                        else:
                            jobtempids, jobtempfilenames = cloud.result(seller['itemdetailjobid'])
                            filenames += jobtempfilenames
                            itemdetailjobids += jobtempids
                            seller['itemdetailjoblist'] = jobtempids
                            seller['jobcompletecheck'] = False
                            seller['filenamelist'] = jobtempfilenames
                except Exception, e:
                    update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
    
            #cloud.join(itemdetailjobids,ignore_errors=False)
    
            successfullsellerfilenamelist=self.check_job_itemdetail_status()
            #self.delete_job_related_data_in_picloud(jobids, mainjobids, itemdetailjobids)
        
        except Exception,e:
            print e    
        finally:
            for seller in self.successsellers:
                if cloud.status(seller['jobid']) not in ('done','error', 'stalled', 'killed'):
                    cloud.kill(seller['jobid'])
                    update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
                    logger.critical("Job of "+str(seller['sellerid'])+" killed although running as there were exception from client rest call")
            removelockfile()
            return successfullsellerfilenamelist
    
    def delete_job_related_data_in_picloud(self, sellerjobids, queuejobdis, itemdetailjobids):
        if itemdetailjobids:
            cloud.delete(sellerjobids)
        if queuejobdis:
            cloud.delete(queuejobdis)
        if itemdetailjobids:
            cloud.delete(itemdetailjobids)
        
    def check_job_itemdetail_status(self):  
        selleritemdetailsjobs_completed = 0
        newsellerlist=[]
        filenamelist=[]
        for seller in self.successsellers:
            if seller.get('jobcompletecheck', True) == False:
                newsellerlist.append(seller)
            else:
                cloud.cloud.cloudLog.critical("job failed for seller "+str(seller))
                if seller.get('itemdetailjobid'):cloud.kill(seller['itemdetailjobid'])
                if seller.get('itemdetailjoblist'):cloud.kill(seller['itemdetailjoblist'])
                
        while selleritemdetailsjobs_completed != len(newsellerlist):
            try:
                for seller in newsellerlist:
                    if seller.get('jobcompletecheck', True) == False:
                        sellerjobfailedcount = 0
                        sellercompletedjobs = 0
                        for jobid in seller['itemdetailjoblist']:
                            if cloud.status(jobid) in ('error', 'stalled', 'killed'):
                                sellerjobfailedcount += 1
                                sellercompletedjobs += 1
                            if cloud.status(jobid) in ('done'):
                                sellercompletedjobs += 1
                        if sellerjobfailedcount > SELLERITEMDETAIL_ALLOWED_JOB_FAIL_COUNT:
                            update_sellers_status([seller], dict(phase="INDEXING", progress="ERROR"))
                            cloud.kill(seller['itemdetailjoblist'])
                            selleritemdetailsjobs_completed += 1
                            seller['jobcompletecheck'] = True
                            logger.exception(seller['sellerid'] + " jobs kill after two job failed")
                        elif sellercompletedjobs >= len(seller['itemdetailjoblist']):
                            selleritemdetailsjobs_completed += 1
                            seller['jobcompletecheck'] = True
                            cloud.delete(seller['itemdetailjobid'])
                            self.download_dumps(seller['filenamelist'])
                            filenamelist+=seller['filenamelist']
                            update_sellers_status([seller], dict(phase="INDEXING", progress="ENDED"))
                print "Job detail wait loop"
                time.sleep(3)
            except Exception, e:
                print e
        
        return filenamelist
                
#    def start_index_from_current_machine(self,etlconfiguration):
#        jobids = []
#        self.successsellers=[]
#        for seller in self.sellers:
#            try:
#                jobids.append(build_index(sellerdata=seller, configuration=self.configuration,etlconfiguration=etlconfiguration))
#                self.successsellers.append(seller)
#            except Exception, e:
#                logger.exception(str(e))
#        filenames=[]
#        itemdetailjobids=[]
#        cloud.join(jobids,ignore_errors=False)
#        for jobid in jobids:
#            jobtempids,jobtempfilenames=cloud.result(jobid, ignore_errors=False)
#            filenames+=jobtempfilenames
#            itemdetailjobids+=jobtempids
#        cloud.join(itemdetailjobids,ignore_errors=False)
#        self.download_dumps(filenames)
#        return filenames

    def download_dumps(self, filenamelist):
        try:
            dumplocation = get_dump_location()
            for filename in filenamelist:
                try:
                    if cloud.files.exists(filename):
                        cloud.files.get(filename, os.path.join(dumplocation, filename))
                        cloud.files.delete(filename)
                    else:
                        print "Not found"
                except Exception, e:
                    logger.exception(str(e))
        except Exception, e:
            logger.exception(str(e))
    

    
        
if __name__ == "__main__":
    pass
        
    

        
