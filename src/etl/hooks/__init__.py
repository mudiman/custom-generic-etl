import datetime



from etl.config import config, logger, BESTMATCHONLY
from etl.dal import reset_sellerdata, update_categories_inventory, update_sellers_inventory, \
    update_sellers_status, get_all_sellers_successfull_sellers_to_update, \
    update_sellers_timestart
from etl.loaders.loaddata import loaddata_from_dump_to_mysql_and_clear_dumps
import os
from etl.dal.dalebay import check_seller
from etl.config.loadconfig import COMPANY_CONFIG
import sys


def confirm_environment_before_etl():
    if not os.environ.get('COMPANY_CONFIG'):
        return False
    else:
        return True

def check_all_sellers(sellers):
    for i, seller in enumerate(sellers):
            if check_seller(sellerid=seller['ebaysellerid'], locale=seller['locale']) == False:
                del sellers[i]
    if len(sellers) == 0:
        raise Exception("No correct sellers available for etl")
    return sellers


def check_token():
    from etl.config.extractor.ebay import config
    from etl.extractors.ebay import EBayExtractor
    from etl.tranformers.ebay import EBayTransformers
    
    ebayconfig=config
    extractor = EBayExtractor()
    transformer = EBayTransformers()
    xml = extractor.get_check_dev_token(token=str(ebayconfig['token']))
    try:
        result = transformer.transform_check_token(xml=xml)
        if result == False:
            print "Developer token is invalid"
            logger.info("Developer token is invalid" + str(datetime.datetime.now()))
            return False
    except Exception, e:
        print "Developer token is invalid "+str(e)
        logger.info("Developer token is invalid" + str(datetime.datetime.now()))
        result = False
    return True

def pre_driver(func):
    def wrapped(*args, **kwargs):
        
        if not confirm_environment_before_etl():
            raise Exception("Raise no environment")
        if not check_token():
            sys.exit()
        logger.info("Start time " + str(datetime.datetime.now()))
        sellers = kwargs.get('sellerdatalist')
        sellers = check_all_sellers(sellers)
        logger.info("Etl started for "+str(sellers))
        update_sellers_timestart(sellers)
        return func(*args, **kwargs)
    return wrapped


def post_driver(func):
    def wrapped(*args, **kwargs):
        
        filenamelist = func(*args, **kwargs)
        logger.info("Driver completed completed time " + str(datetime.datetime.now()))
        etlconfiguration = kwargs.get('etlconfiguration', "ALL")
        
        sellers = kwargs.get('sellerdatalist')
        
        successsellers = get_all_sellers_successfull_sellers_to_update(sellers)
        
        if len(successsellers) == 0:
            raise Exception("No successful sellers")
        
        logger.info("successfull sellers  "+str(successsellers))
        
        if etlconfiguration != BESTMATCHONLY:
            reset_sellerdata(successsellers)
            
        loaddata_from_dump_to_mysql_and_clear_dumps(filenamelist)
        
        update_categories_inventory(successsellers)
        update_sellers_inventory(successsellers)
        progress = dict(PHASE="DB", PROGRESS="UPDATED")
        update_sellers_status(successsellers, progress)
        
        logger.info("Complete time " + str(datetime.datetime.now()))
        
        return successsellers
    return wrapped
