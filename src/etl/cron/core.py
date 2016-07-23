'''
Created on Aug 24, 2011

@author: Mudassar Ali
'''

from httplib2 import Http
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


  
from etl.config.drivers.picloud import removelockfile
from etl.loaders.dumps import get_dump_location
from etl.miscfunctions import check_match_string_for_running_process
from etl.dal.sisdal import get_facet_field, aggregate_field_value
from etl.config.drivers import DRIVERLISTNAMES, THREADEDEBAY, PICLOUDEBAY
from etl.dal.smsdal import categories_properties
from etl.dal.scoringdal import calculate_score
from etl.dal import get_seller_by_id, get_all_leafcategories, insert_suggestion, \
    truncatesuggestion, update_sellers_status, \
    update_sellers_etl_scoring_or_solrindexing_time
from etl.drivers.rundriver import start_driver
from etl.config.loader.mysql import Connection
from etl.dal.solr import updateSolr, resetSolr, getstatsoffields
from etl.config import logger, config, COMPANY_ITEM_SERVICE_ACCOUNT_OVERVIEW, \
    OVERVIEWTHRESHOLD
from etl.config.loadconfig import COMPANY_CONFIG
from etl.system.system import resetwebservers, initialize,\
    delete_oldfiles_directory_from_path


def etl_scoring_index(sellerid, drivertype, domain, EBAY_ENVIRONMENT, etl=True, bestmatchonly=False, scoring=True, index=True, updatesuggestion=False, bypassrecentcheck=False):
    try:
        progress = {}
        progress['PROGRESS'] = "START"
        successsellers = []
        progress = {}
        if etl or bestmatchonly:
            temp = get_seller_by_id(sellerid)
            if etl:
                print "etl " + sellerid
                logger.info("etl " + sellerid)
                successsellers = start_driver(driverins=drivertype, sellerdatalist=[temp])

            if bestmatchonly:
                print "bestmatchonly " + sellerid
                logger.info("bestmatchonly " + sellerid)
                successsellers = start_driver(driverins=drivertype, sellerdatalist=[temp], etlconfiguration="BESTMATCHONLY")
            
            
            resetwebservers()

        if scoring == True:
            print "scoring " + sellerid
            logger.info("Score Calculating " + sellerid)
            progress = dict(PHASE="SCORING", PROGRESS="STARTED")
            update_sellers_status([{'sellerid':sellerid}], progress)
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SCORING", "start")
            calculate_score(sellerid)
            progress = dict(PHASE="SCORING", PROGRESS="ENDED")
            update_sellers_status([{'sellerid':sellerid}], progress)
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SCORING", "end")

    
    
        if index == True:
            print "Indexing " + sellerid
            logger.info("Solr Indexing " + sellerid)
            progress = dict(PHASE="SOLRINDEXING", PROGRESS="STARTED")
            update_sellers_status([{'sellerid':sellerid}], progress)
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SOLRINDEXING", "start")
            resetSolr(sellerid)
            updateSolr(sellerid, config.ENVIRONMENT)
            progress = dict(PHASE="SOLRINDEXING", PROGRESS="ENDED")
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SOLRINDEXING", "end")
            update_sellers_status([{'sellerid':sellerid}], progress)

        
    

        if updatesuggestion:
            
            progress = dict(PHASE="SUGGESSTIONINDEXING", PROGRESS="STARTED")
            update_sellers_status([{'sellerid':sellerid}], progress)
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SUGGESSTIONINDEXING", "start")
            
            update_suggestion(sellerid)
            
            progress = dict(PHASE="SUGGESSTIONINDEXING", PROGRESS="ENDED")
            update_sellers_etl_scoring_or_solrindexing_time([{'sellerid':sellerid}], "SUGGESSTIONINDEXING", "end")
            update_sellers_status([{'sellerid':sellerid}], progress)

            
    except Exception, e:
            progress['PROGRESS'] = "ERROR"
            update_sellers_status([{'sellerid':sellerid}], progress)
            logger.critical(str(e))    



        

def update_account_overview():
    os.system("python %s %s %d" % (COMPANY_ITEM_SERVICE_ACCOUNT_OVERVIEW, config.ENVIRONMENT, OVERVIEWTHRESHOLD))
    

def update_suggestion(sellerid=None):
    truncatesuggestion(sellerid)
    
    categories = get_all_leafcategories(sellerid)
    
    for category in categories:
        try:
            sellerid = str(category['sellerid'])
            locale = str(category['sellerid']).split('@')[1]
            categoryid = str(category['categoryid'])
            
            print "update suggestions for sellerid %s for category %s" % (sellerid, categoryid)
            
            fieldslist = categories_properties(categoryid, locale)
            varfieldlist = []
            for field in fieldslist:
                varfieldlist.append("var_" + field)
                        
            facet_fields = get_facet_field(sellerid, categoryid, fieldslist)
            var_facet_fields = get_facet_field(sellerid, categoryid, varfieldlist)
            
            
            for field, value in facet_fields.items():
                try:
                    res = aggregate_field_value(value)
                    varvalue = var_facet_fields.get("var_" + field)
                    if not varvalue:varvalue = dict()
                    varres = aggregate_field_value(varvalue)
                    res['missing'] += varres.get('missing', 0)
                    res['invalid'] += varres.get('invalid', 0)
                    
                    if res['missing'] > 0:
                        stats = getstatsoffields(sellerid, categoryid, "\"(missing)\"", field)
                        varstats = getstatsoffields(sellerid, categoryid, "\"(missing)\"", "var_" + field)
                        for key in stats:
                            stats[key] += varstats[key]
                        insert_suggestion(sellerid, categoryid, field, "missing", res['missing'], stats['inventoryValue'], stats['sales'])
                    if res['invalid'] > 0:
                        stats = getstatsoffields(sellerid, categoryid, 'COMPANY-invalid-$*', field)
                        varstats = getstatsoffields(sellerid, categoryid, 'COMPANY-invalid-$*', "var_" + field)
                        for key in stats:
                            stats[key] += varstats[key]
                        insert_suggestion(sellerid, categoryid, field, "invalid", res['invalid'], stats['inventoryValue'], stats['sales'])
                except Exception, e:
                    logger.exception(str(e))
                    
        except Exception, e:
            logger.exception(str(e))
        
    

def check_pythonfile_running(filename):
    if check_match_string_for_running_process("ps -ef|grep python",filename):
        print "Job already running so exist new job"
        logger.warning("Job already running so exiting new job")
        sys.exit()    

def check_process_running(sellerid):
    if check_match_string_for_running_process("ps -ef|grep python",sellerid):
        print "Job already running so exist new job"
        logger.warning("Job already running so exiting new job")
        sys.exit()


def single_seller_job():
    try:
        if len(sys.argv) < 9:
            print """please specify complete command like "python filename.py sellerid drivertype("""+PICLOUDEBAY+""","""+THREADEDEBAY+""") environment etl(1) bestmatch(1) scoring(1) index(1) overviewupdate(1)" """
            sys.exit()
        
        delete_oldfiles_directory_from_path(get_dump_location())
        
        sellerid = sys.argv[1]
        drivertype = sys.argv[2]
        Environment = sys.argv[3]
        etl = bool(int(sys.argv[4]))
        bestmatchonly = bool(int(sys.argv[5]))
        scoring = bool(int(sys.argv[6]))
        index = bool(int(sys.argv[7]))
        updateoverview = bool(int(sys.argv[8]))
        updatesuggestion = bool(int(sys.argv[9]))
        
        
        check_process_running(sellerid)
        
        if not drivertype in DRIVERLISTNAMES:
            print "No Such driver"
            sys.exit()
            
        if etl == True:
            bestmatchonly = False
            
        initialize(Environment,drivertype=drivertype)
        
        temp = get_seller_by_id(sellerid)
        if not temp:
            print "seller not available in db  or its job might be already running check db status field %s " % sellerid
            sys.exit()
              
        domain = "ebay.company.com"
        
        
        etl_scoring_index(sellerid, drivertype, domain, 'PRODUCTION', etl, bestmatchonly, scoring, index, updatesuggestion, bypassrecentcheck=True)
        
        if updateoverview:
            update_account_overview()
        
#        if updatesuggestion:
#            update_suggestion()
        #resetwebservers()

    except Exception, e:
        logger.exception(str(e))
    finally:
        try:
            Connection(config.ENVIRONMENT).mysql_connection.close()
        except Exception,e:
            pass
        removelockfile()
        sys.exit()
        os.kill(os.getpid(), 9)
 
              
if __name__ == '__main__':
    initialize("DEVELOPMENT")
    #single_seller_job()
    update_suggestion(sellerid="littlewoods-clearance@GB")
        
        

