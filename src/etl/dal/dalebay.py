'''
Created on Mar 30, 2012

@author: mudassar
'''
from etl.extractors.ebay.findingapi import get_categories
from elementtree.ElementTree import fromstring
from etl.tranformers.ebay.checks import remove_namespace
from etl.config.drivers.picloud.ebay import __BLANK_COMPANY_SELLER__

def check_seller(sellerid, locale):
    if sellerid == __BLANK_COMPANY_SELLER__:
        return True
    else:
        data = get_categories(sellerid=sellerid, selleridkey=None, categoryid=None, locale=locale, pageNumber=1, entriesPerPage=5)
        xml = fromstring(data)
        namespace = 'http://www.ebay.com/marketplace/search/v1/services'
        remove_namespace(xml, namespace)
        if xml.findtext('ack') == "Failure":
            print xml.findtext('errorMessage/error/message')
            return False
        else:
            return True
    

if __name__ == '__main__':
    check_seller('barratts-priceless', 'GB')
