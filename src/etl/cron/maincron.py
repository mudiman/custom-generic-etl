import MySQLdb
import os
import sys
from etl.config.drivers.picloud import removelockfile
from etl.loaders.dumps import get_dump_location





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
    
from etl.cron.core import etl_scoring_index, resetwebservers, update_account_overview, \
    update_suggestion
from etl.config.loader.mysql import Connection
from etl.config import logger, config
from etl.drivers.rundriver import start_driver
from etl.dal import get_all_sellers_for_cron,\
    get_all_sellers_unsuccessfull_sellers_to_update
from etl.system.system import initialize, delete_oldfiles_directory_from_path
from etl.config.drivers import DRIVERLISTNAMES, THREADEDEBAY, PICLOUDEBAY



def get_input_for_cronjob():
    logger.info("Cron Started")
    
    Environment = 'DEVELOPMENT'
    domain = "ebay.company.com"
    etl = True
    bestmatchonly = False
    scoring = True
    index = True
    overview = True
    suggestion = False
    Drivertype = THREADEDEBAY
    cronupdate = 999
    
    try:
        if len(sys.argv) > 1:cronupdate = sys.argv[1]
        if len(sys.argv) > 2:Drivertype = sys.argv[2]
        if len(sys.argv) > 3:Environment = sys.argv[3]
        if len(sys.argv) > 4:etl = bool(int(sys.argv[4]))
        if len(sys.argv) > 5:bestmatchonly = bool(int(sys.argv[5]))
        if len(sys.argv) > 6:scoring = bool(int(sys.argv[6]))
        if len(sys.argv) > 7:index = bool(int(sys.argv[7]))
        if len(sys.argv) > 8:overview = bool(int(sys.argv[8]))
        if len(sys.argv) > 9:suggestion = bool(int(sys.argv[9]))
        initialize(Environment)
        
        
        if not Drivertype in DRIVERLISTNAMES:
            print "No Such driver"
            sys.exit()
    except Exception, e:
        print """please specify all argument drivers name plus source of extraction\
        \nfor example\n "python filename.py cronupdateno() drivertype("""+PICLOUDEBAY+""","""+THREADEDEBAY+""") environment etl(1) bestmatch(1) scoring(1) index(1) overviewupdate(1)" """
        sys.exit() 
    finally:
        return (cronupdate,domain,Drivertype,Environment,etl,bestmatchonly,scoring,index,overview,suggestion)
        
def runcronforsellerwith():
    try:
        logger.debug("Cronjob started")
        removelockfile()
        delete_oldfiles_directory_from_path(get_dump_location())
        cronupdate,domain,Drivertype,Environment,etl,bestmatchonly,scoring,index,overview,suggestion=get_input_for_cronjob()
        logger.debug("Cronupdate value for this job is %s" % str(cronupdate))
        sellers = get_all_sellers_for_cron(cronupdate=cronupdate)
        runjobforsellers(sellers,Drivertype,domain,Environment,etl,bestmatchonly,scoring,index,overview,suggestion)
        
        #print "Running failed seller on threaded"
        logger.info("Running failed seller on threaded")
        failedsellers=get_all_sellers_unsuccessfull_sellers_to_update(sellers)
        runjobforsellers(failedsellers,THREADEDEBAY,domain,Environment,etl,bestmatchonly,scoring,index,overview,suggestion)
        
    except Exception, e:
        logger.exception(str(e))
    finally:
        Connection(Environment).mysql_connection.close()
        removelockfile()
        sys.exit()
        os.kill(os.getpid(), 9)

     

def runjobforsellers(sellers,Drivertype,domain,Environment,etl,bestmatchonly,scoring,index,overview,suggestion):
    try:
        if len(sellers) == 0:
            print "No sellers for this job"
            logger.debug("No sellers for this job")
            sys.exit()
        sourcename = "ebay"
        successsellers = []
        if etl:
            successsellers = start_driver(driverins=Drivertype, sellerdatalist=sellers)
        else:
            successsellers = sellers
        
        resetwebservers()
        
        for seller in successsellers:
            print seller['sellerid']           
            try:                
                etl_scoring_index(seller['sellerid'], Drivertype, domain, 'PRODUCTION', False, bestmatchonly, scoring, index, suggestion)
            except Exception, e:
                logger.critical(str(e))

   
        if overview:
            update_account_overview()
        
            
        resetwebservers()
        
    except Exception, e:
        logger.exception(str(e))
