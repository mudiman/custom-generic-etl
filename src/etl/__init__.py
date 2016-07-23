'''
Created on Oct 21, 2011

@author: Mudassar
'''


from etl.config import logger
from etl.miscfunctions import create_list, log_critical_message,\
    log_exception_message
from etl.exceptions import ExtractorException, TransformerException
import sys



def get_categories(**kwargs):
    """
        Get categories from ebay with sellerid and category and locale
    """
    try:
        etldriver = kwargs['etldriver']
        xml = etldriver.extractor.get_categories(**kwargs)
        kwargs['xml'] = xml
        categorydata = etldriver.transformer.transform_get_categories(**kwargs)
        return categorydata
    except ExtractorException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    except TransformerException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    except Exception, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    
def get_items(**kwargs):
    """
        Get items from ebay with sellerid and category and locale
    """
    try:
        res = []
        etldriver = kwargs['etldriver']
        xml = etldriver.extractor.get_items(**kwargs)
        kwargs['xml'] = xml
        itemlist = etldriver.transformer.transform_get_items(**kwargs)
        kwargs['datatype'] = 'item'
        kwargs['data'] = itemlist
        etldriver.loader.writedata(**kwargs)
        res = [item['itemId'] for item in itemlist]
        return list(set(res))
    except ExtractorException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    except TransformerException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
    except Exception, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e

            
def get_item_details(**kwargs):
    """
        Get tradingapi item detail of list of itemids specified
    """
    try:
        etldriver = kwargs['etldriver']
        xml = etldriver.extractor.get_item_detail(**kwargs)
        kwargs['xml'] = xml
        results = etldriver.transformer.transform_get_item_detail(**kwargs)
        log_critical_message("done transformation ")
        kwargs['data'] = results
        kwargs['datatype'] = 'itemdetail'
        etldriver.loader.writedata(**kwargs)
        return
    except ExtractorException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    except TransformerException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
    except Exception, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e

def get_item_analytics(**kwargs):
    try:
        etldriver = kwargs['etldriver']
        xml = etldriver.extractor.get_item_analytics(**kwargs)
        kwargs['xml'] = xml
        results = etldriver.transformer.transform_get_item_analytics(**kwargs)
        kwargs['data'] = results
        kwargs['datatype'] = 'bestmatch'
        etldriver.loader.writedata(**kwargs)
        return
    except ExtractorException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e
    except TransformerException, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
    except Exception, e:
        print e
        log_exception_message(str(e),etldriver.customlogger)
        raise e




if __name__ == '__main__':
    pass
    
