'''
Created on Apr 24, 2012

@author: mudassar
'''
import sys
import datetime
import time





try:
    import esync
except ImportError:
    import os
    import sys
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    print p
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from esync.driver.main import reading_inbox, deducing_items_and_categories, \
    fetch_item_details, update_database, calculate_score, bubbleup_data, \
    reindex_affected_items, save_revised_items, register_new_categories, logger,\
    update_bestmatch_for_whole_seller
from etl.system.system import initialize, delete_oldfiles_directory_from_path
from esync.driver.recordchanges import record_changes
from esync.dumps import get_dump_location
from etl.miscfunctions import check_match_string_for_running_process
from etl.cron.core import update_account_overview


#FIXME:Puth the path in config 
def start_sync(date=""):
    reading_inbox(date=date, callback="yes")
    deducing_items_and_categories(date=date, callback="yes")
    save_revised_items(date=date)
    register_new_categories(date=date)
    fetch_item_details(date=date)
    update_database(date=date)
    update_bestmatch_for_whole_seller(date=date)
    calculate_score(date=date)
    bubbleup_data(date=date)
    reindex_affected_items(date=date)
    record_changes(date=date)
    update_account_overview()
     

if __name__ == '__main__':
    print "Notification job started"
    logger.info("Notification job started")
    if check_match_string_for_running_process("ps -ef|grep python","esync/jobs/run.py"):
        print "Job already running so exiting new job"
        logger.warning("Job already running so exiting new job")
        sys.exit()
    
    initialize("DEVELOPMENT")
    if len(sys.argv)>1:
        date=sys.argv[1]
    else:
        now = datetime.datetime.now()
        date = "%(year)s-%(month)s-%(day)s-%(hour)s" % ({
                                                    "year":str(now.year),
                                                    "month":str(now.month),
                                                    "day":str(now.day),
                                                    "hour":str(now.hour)   
                                                    })
    
    delete_oldfiles_directory_from_path(get_dump_location())
    start_sync(date=date)
    
