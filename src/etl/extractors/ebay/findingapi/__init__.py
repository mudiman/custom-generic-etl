'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''
from httplib2 import Http
from etl.exceptions import ExtractorException
from etl.config import logger
from etl.miscfunctions import cache_raw_data, httpcallswrapper
from etl.config.extractor.ebay import config
from etl.config.drivers.picloud.ebay import __BLANK_COMPANY_SELLER__




@cache_raw_data
def get_categories(sellerid=None, selleridkey=None, categoryid=None, locale=None, pageNumber=1, entriesPerPage=5):
    """
        Gets ebay findingapi findingitemadvance response xml with aspect and category histogram
    """
    try:
        rawbody = ""
        url = "http://" + config['server'] + ".ebay.com/services/search/FindingService/v1?OPERATION-NAME=findItemsAdvanced&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=" + config['appid'] + "&GLOBAL-ID=EBAY-" + str(locale) + "&RESPONSE-DATA-FORMAT=XML&outputSelector(0)=CategoryHistogram&outputSelector(1)=AspectHistogram"
        if categoryid:
            url = url + "&categoryId=" + str(categoryid)
        if entriesPerPage:
            url = url + "&paginationInput.entriesPerPage=" + str(entriesPerPage)
        if pageNumber:
            url = url + "&paginationInput.pageNumber=" + str(pageNumber)
        if sellerid:
            url = url + "&itemFilter(0).name=Seller&itemFilter(0).value=" + sellerid            
        try:
            responseheaders, rawbody = httpcallswrapper(url=url, method="GET", body=None, headers=None)
        except Exception, e:
            logger.debug(str(e))
            raise ExtractorException("Exception from ebay response %s" % str(e))
    except ExtractorException:
        raise
    except Exception , e:
        logger.exception(str(e))
    finally:
        return rawbody

@cache_raw_data
def get_items(sellerid=None, selleridkey=None, categoryid=None, locale=None, pageNumber=1, entriesPerPage=100):
    """
        Gets ebay findingapi findingitemadvanceresponse xml with items
    """
    try:
        rawbody = ""
        url = "http://" + config['server'] + ".ebay.com/services/search/FindingService/v1?OPERATION-NAME=findItemsAdvanced&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=" + config['appid'] + "&GLOBAL-ID=EBAY-" + str(locale) + "&RESPONSE-DATA-FORMAT=XML&outputSelector(0)=StoreInfo&outputSelector(1)=SellerInfo"
        if categoryid:
            url = url + "&categoryId=" + str(categoryid)
        url = url + "&paginationInput.entriesPerPage=" + str(entriesPerPage)
        url = url + "&paginationInput.pageNumber=" + str(pageNumber)
        if sellerid and selleridkey != __BLANK_COMPANY_SELLER__:
            url = url + "&itemFilter(0).name=Seller&itemFilter(0).value=" + sellerid            
        try:
            responseheaders, rawbody = httpcallswrapper(url=url, method="GET", body=None, headers=None)
        except Exception, e:
            logger.debug(str(e))
            raise ExtractorException("Exception from ebay response %s" % str(e))
    except ExtractorException:
        raise
    except Exception , e:
        logger.exception(str(e))
        raise
    finally:
        return rawbody
    

if __name__ == '__main__':
    pass
    #print get_categories(sellerid="__BLANK_COMPANY_SELLER__@US", categoryid=None, locale="GB")
    #print get_categories(sellerid="__BLANK_COMPANY_SELLER__@US", categoryid="11450", locale="GB")
    print get_items(sellerid="calvinklein@US", selleridkey="__BLANK_COMPANY_SELLER__@US", categoryid="11498", locale="GB", pageNumber=1, entriesPerPage=100)
