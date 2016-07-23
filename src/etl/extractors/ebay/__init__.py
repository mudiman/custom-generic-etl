'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''


from etl.extractors.generic import ExtractorSkeleton
from etl.extractors.ebay.findingapi import get_categories, get_items
from etl.extractors.ebay.tradingapi import get_item_detail, check_token, \
    token_user, check_dev_token
from etl.extractors.ebay.bestmatchapi import get_items_bestmatch_data
from etl.miscfunctions import create_list
from etl.hooks.etljobhooks import class_logging_decorator
    

class EBayExtractor(ExtractorSkeleton):

    @class_logging_decorator
    def get_categories(self, **kwargs):
        sellerid = kwargs.get('sellerid', None)
        selleridkey = kwargs.get('selleridkey', None)
        categoryid = kwargs.get('categoryid', None)
        locale = kwargs.get('locale', None)
        pageNumber = kwargs.get('pageNumber', None)
        entriesPerPage = kwargs.get('entriesPerPage', None)
        return get_categories(sellerid=sellerid, selleridkey=selleridkey, categoryid=categoryid, locale=locale, pageNumber=pageNumber, entriesPerPage=entriesPerPage)
    
    @class_logging_decorator
    def get_items(self, **kwargs):
        sellerid = kwargs.get('sellerid', None)
        categoryid = kwargs.get('categoryid', None)
        selleridkey = kwargs.get('selleridkey', None)
        locale = kwargs.get('locale', None)
        offset = kwargs.get('offset', 1)
        limit = kwargs.get('limit', 100)
        entriesPerPage = 100
        xml = []
        pages = int(limit / entriesPerPage)
        if limit % entriesPerPage > 0:pages += 1;
        startpageno = offset / entriesPerPage
        if startpageno == 0: startpageno = 1
        i = startpageno
        while True:
            temp = get_items(sellerid=sellerid, selleridkey=selleridkey, categoryid=categoryid, locale=locale, pageNumber=i, entriesPerPage=entriesPerPage)
            xml.append(temp)
            i += 1
            if i > pages:break
            
        return xml
    
    @class_logging_decorator
    def get_item_detail(self, **kwargs):
        itemid = kwargs.get('itemid', None)
        itemids = create_list(itemid)
        xmllist = []
        for item in itemids:
            try:
                xml = get_item_detail(itemid=item)
                xmllist.append(xml)
            except Exception, e:
                print e
        return xmllist
    
    @class_logging_decorator
    def get_check_token(self, **kwargs):
        token = kwargs.get('token', None)
        xml = check_token(token=token)
        return xml

    @class_logging_decorator
    def get_check_dev_token(self, **kwargs):
        token = kwargs.get('token', None)
        xml = check_dev_token(token=token)
        return xml
        
    @class_logging_decorator
    def get_token_user(self, **kwargs):
        token = kwargs.get('token', None)
        xml = token_user(token=token)
        return xml
    
    @class_logging_decorator
    def get_item_analytics(self, **kwargs):
        locale = kwargs.get('locale', None)
        token = kwargs.get('token', None)
        itemids = kwargs.get('itemid', None)
        itemids = create_list(itemids)
        xmllist = []
        listof50items = [itemids[i:i + 50] for i in range(0, len(itemids), 50)]
        for item50 in listof50items:
            try:
                arr = ''.join(["<itemId>" + str(i) + "</itemId>" for i in item50])
                xml = get_items_bestmatch_data(locale=locale, token=token, itemid=arr)
                xmllist.append(xml)
            except Exception, e:
                print e
        return xmllist
    

