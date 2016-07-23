'''
Created on May 22, 2012

@author: mudassar
'''

import os
import sys



try:
    import etl
except ImportError:
    
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from etl.dal import get_all_new_sellers, get_all_sellers, \
    get_trail_user_sellerids, get_all_sellers_unsuccessfull_sellers_to_update
from etl.cron.core import etl_scoring_index, resetwebservers, update_account_overview,\
    check_pythonfile_running
from etl.config.loader.mysql import Connection
from etl.config import logger, config
from etl.drivers.rundriver import start_driver
from etl.dal import get_all_sellers_for_cron
from etl.system.system import initialize
from etl.config.drivers import DRIVERLISTNAMES, THREADEDEBAY
from etl.cron.maincron import runjobforsellers


def check_existing_job(sellers):
    sellerendstatus = ["""{"PHASE": "SCORING", "PROGRESS": "ENDED"}""","""{"PHASE": "INDEXING", "PROGRESS": "ERROR"}""", """{"PHASE": "SCORING", "PROGRESS": "ENDING"}""", """{"PHASE": "SOLRINDEXING", "PROGRESS": "ENDED"}""", """{"PHASE": "SOLRINDEXING", "PROGRESS": "ENDING"}""", """{"phase": "NEVER_STARTED", "progress": "None"}""", """{"PHASE": "SUGGESSTIONINDEXING", "PROGRESS": "ENDED"}""","""{"PROGRESS": "ERROR"}"""]
    for seller in sellers:
        if not seller['status'] in sellerendstatus:
            logger.critical(seller['sellerid'] + " job is running " + seller['status'])
            return True
    return False


def runcronfornewseller(sellers):
    Environment = 'DEVELOPMENT'
    domain = "ebay.company.com"
    etl = True
    bestmatchonly = False
    scoring = True
    index = True
    overview = True
    suggestion = False
    Drivertype = "picloudebay"
    cronupdate = 999
        
    sellers = get_all_new_sellers(sellers)
    runjobforsellers(sellers,Drivertype,domain,Environment,etl,bestmatchonly,scoring,index,overview,suggestion)
    
    # running failed seller with THREADEDEBAY
    #print "Running failed seller on threaded"
    logger.info("Running failed seller on threaded")
    failedsellers=get_all_sellers_unsuccessfull_sellers_to_update(sellers)
    runjobforsellers(failedsellers,THREADEDEBAY,domain,Environment,etl,bestmatchonly,scoring,index,overview,suggestion)
        
        
if __name__ == '__main__':
    initialize("DEVELOPMENT")
    logger.critical("started new seller job")
    check_pythonfile_running("newsellersjob")
#    sellers = get_all_sellers()
#    if check_existing_job(sellers) == False:
#        sellerids = get_trail_user_sellerids()
#        if len(sellerids) > 0:
#            runcronfornewseller(sellerids)
#        else:
#            logger.critical("No seller left for full etl")
#    else:
#        logger.critical("Existing job is running or status of some seller shows some phase not ended")

    sellerids = get_trail_user_sellerids()
    if len(sellerids) > 0:
        runcronfornewseller(sellerids)
    else:
        logger.critical("No seller left for full etl")
