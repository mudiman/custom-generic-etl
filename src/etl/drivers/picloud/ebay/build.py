'''
Created on Oct 26, 2011

@author: Mudassar Ali
'''
import etl
from etl.drivers.picloud.ebay import PiCloudEBayETLDriver
from etl.loaders.dumps import make_file_name
import cloud
import json
from etl.miscfunctions import  getvalue_from_list_with_key
from etl.config.drivers import EBAY_COUNT_LIMIT
from types import ListType
from etl.dal import get_sellers_items

class BuildLogic(object):
    '''
    classdocs
    '''


    def __init__(self, sellerdata, configuration, etldriver, etlconfiguration, itemids=None):
        '''
        Constructor
        '''
        self.sellerid = sellerdata.get('ebaysellerid')
        self.selleridkey = sellerdata.get('sellerid')
        self.locale = sellerdata.get('locale')
        self.token = sellerdata.get('token')
        self.etl = sellerdata.get('etl', {})
        self.itemids = itemids;
        self.etlconfiguration = etlconfiguration
        self.etldriver = etldriver
        self.configuration = configuration
        if self.token == "None":self.token = None
        if self.token:
            if self.check_token(sellerid=self.sellerid) == False:
                self.token = None
        
    
    def check_token(self, sellerid=None):
        xml = self.etldriver.extractor.get_check_token(token=self.token)
        try:
            result = self.etldriver.transformer.transform_check_token(xml=xml)
            if result == False:
                return False
            xml = self.etldriver.extractor.get_token_user(token=self.token)
            result = self.etldriver.transformer.transform_check_user_with_token(xml=xml)
            if result == False:
                return False
            if result != sellerid:
                return False
        except Exception, e:
            cloud.cloud.cloudLog.exception(str(e))
            result = False
        return result
        
    def build_category_tree(self):
        """
            Get items from ebay with sellerid and category and locale
        """
        categorystack = []
        writerlist = []
        leaflist = []
        leaf = 0
        categorydata = etl.get_categories(etldriver=self.etldriver, sellerid=self.sellerid, selleridkey=self.selleridkey, locale=self.locale)
        for eachcategory in categorydata['childcategories']:
            categorystack.append(eachcategory)
        while len(categorystack) != 0:
            try:
                childcategory = categorystack.pop()
                childcategorydata = etl.get_categories(etldriver=self.etldriver, sellerid=self.sellerid, selleridkey=self.selleridkey, categoryid=childcategory['categoryid'], locale=self.locale)
                if len(childcategorydata['childcategories']) == 0:
                    leaf = {}
                    leaf[childcategory['categoryid']] = childcategory['count']
                    leaflist.append(leaf)
                else:
                    for subchild in childcategorydata['childcategories']:
                        categorystack.append(subchild)
                
                childcategory['aspect'] = childcategorydata['aspect']
                childcategory['sellerid'] = childcategorydata['sellerid']
                childcategory['isleaf'] = childcategorydata['isleaf']
                writerlist.append(childcategory)
            except Exception, e:
                cloud.cloud.cloudLog.exception(str(e))
        return [writerlist, leaflist]

    
    
    def get_leaf_categories(self):
        """
            Returns all the leaf level categories in form of dictionary.
            [{'11498': 4569}]
        """
        writelist, leaflist = self.build_category_tree()
        self.etldriver.loader.writedata(sellerid=self.sellerid, locale=self.locale, datatype='categories', data=writelist)
        return leaflist
    


def generate_categories(index=None):
    filenames = []
    filenames.append(make_file_name(sellerid=index.sellerid, locale=index.locale, datatype='categories'))
    leafcategories = index.get_leaf_categories()
    if "categories" in index.etl.keys():
        requiredcategories = index.etl['categories']
        newleafcategories = []
        for requiredcategory in requiredcategories:
            category = {}
            tempcount = getvalue_from_list_with_key(leafcategories, requiredcategory)
            if tempcount < 10000:
                category[requiredcategory] = tempcount
            else:
                category[requiredcategory] = 10000
            newleafcategories.append(category)
        leafcategories = newleafcategories


    return leafcategories, filenames


def generate_finding_calls(index, categoryjobid=None):
    findingjobids = []
    try:
        leafcategories, filenames = cloud.result(categoryjobid)
    except Exception, e:
        cloud.cloud.cloudLog.exception(str(e))
        raise e
    for leafcategory in leafcategories:
        try:
            #counter -= 1
            leafcategoryid = leafcategory.keys()[0]
            count = leafcategory[leafcategoryid]
            if count > EBAY_COUNT_LIMIT:
                cloud.cloud.cloudLog.critical(str(leafcategoryid) + " category item count is greater then 100000")
                count = EBAY_COUNT_LIMIT
            pages = count / index.configuration['get_item_calls_per_machine']
            if (pages % index.configuration['get_item_calls_per_machine']) > 0 :pages += 1;
            i = 0
            finished = True
            while finished:
                try:
                    offset = i * index.configuration['get_item_calls_per_machine']
                    limit = count
                    if pages > 0:
                        limit = index.configuration['get_item_calls_per_machine'] * (i + 1)
                        if limit > count:limit = count
                    findingjobids.append(cloud.call(etl.get_items, etldriver=index.etldriver, sellerid=index.sellerid, selleridkey=index.selleridkey, locale=index.locale, categoryid=leafcategoryid, limit=limit, offset=offset, _type="c1", _max_runtime=index.configuration['get_item_calls_per_machine'] * index.configuration['timeout_per_call'] / 10, _label="GET ITEMS"))
                    filenames.append(make_file_name(sellerid=index.sellerid, locale=index.locale, categoryid=leafcategoryid, limit=limit, offset=offset, datatype="item"))
                    i += 1
                    if i >= pages:
                        finished = False
                except Exception, e:
                    cloud.cloud.cloudLog.exception(str(e))
                    finished = False

        except Exception, e:
            cloud.cloud.cloudLog.exception(str(e))
    return findingjobids, filenames

def generate_items_detail_and_analytics_main_job(index, findingjobids):
    itemdetailjobids = []
    try:
        itemjobids, filenames = cloud.result(findingjobids)
    except Exception, e:
        cloud.cloud.cloudLog.exception(str(e))
        raise e
    itemidlisttemp = cloud.result(itemjobids)
    itemidlist = []
    for itemsublist in itemidlisttemp:
        if type(itemsublist) is ListType:
            itemidlist += itemsublist
        else:
            itemidlist.append(itemsublist)
    getitemdetaillist = [itemidlist[i:i + index.configuration['item_detail_calls_per_machine']] for i in range(0, len(itemidlist), index.configuration['item_detail_calls_per_machine'])]
    for sublist in getitemdetaillist:
        itemdetailjobids.append(cloud.call(etl.get_item_details, etldriver=index.etldriver, itemid=sublist, selleridkey=index.selleridkey, _type="c1", _max_runtime=index.configuration['item_detail_calls_per_machine'] * index.configuration['timeout_per_call'], _label="GET ITEMS DETAILS"))
        filenames.append(make_file_name(itemid=sublist, datatype="itemdetail"))
    if index.token:
        getitembestmatchlist = [itemidlist[i:i + index.configuration['item_analytics_calls_per_machine']] for i in range(0, len(itemidlist), index.configuration['item_analytics_calls_per_machine'])]
        for sublist in getitembestmatchlist:
            itemdetailjobids.append(cloud.call(etl.get_item_analytics, etldriver=index.etldriver, itemid=sublist, locale=index.locale, selleridkey=index.selleridkey, token=index.token, _type="c1", _max_runtime=index.configuration['item_analytics_calls_per_machine'] * index.configuration['timeout_per_call'] / 50, _label="GET ITEMS BESTMATCH"))
            filenames.append(make_file_name(itemid=sublist, locale=index.locale, datatype="bestmatch"))
    return itemdetailjobids, filenames


def generate_items_analytics_main_job(index, itemidlist):
    itemdetailjobids = []
    filenames = []
    if index.token:
        getitembestmatchlist = [itemidlist[i:i + index.configuration['item_analytics_calls_per_machine']] for i in range(0, len(itemidlist), index.configuration['item_analytics_calls_per_machine'])]
        for sublist in getitembestmatchlist:
            itemdetailjobids.append(cloud.call(etl.get_item_analytics, etldriver=index.etldriver, itemid=sublist, selleridkey=index.selleridkey,locale=index.locale, token=index.token, _type="c1", _label="GET ITEMS BESTMATCH"))
            filenames.append(make_file_name(itemid=sublist, locale=index.locale, datatype="bestmatch"))
    return itemdetailjobids, filenames
        
        
def build_index(**kwargs):
    sellerdata = kwargs.get('sellerdata')
    configuration = kwargs.get('configuration')
    etlconfiguration = kwargs.get('etlconfiguration', False)
    itemids = kwargs.get('itemids', [])
    sellerdata['etl'] = json.loads(sellerdata['etl'])
    
    if not configuration:
        from etl.config.drivers.picloud.ebay import picloud_ebay_configuration
        configuration = picloud_ebay_configuration

    index = BuildLogic(sellerdata, configuration, PiCloudEBayETLDriver(), etlconfiguration)
    itemdetailjobid = 0
    
    categoryjobid = cloud.call(generate_categories, index=index, _type="c1", _max_runtime=20, _label="GET GATEGORIES")
    findingjobids = cloud.call(generate_finding_calls, index=index, categoryjobid=categoryjobid, _type="c1", _depends_on=categoryjobid, _label="GENERATE FINDING CALLS")
    #findingjobids=243103
    #get_items_detail_and_analytics_main_job(index=index,findingjobids=findingjobids)
    itemdetailjobid = cloud.call(generate_items_detail_and_analytics_main_job, index=index, findingjobids=findingjobids, _type="c1", _depends_on=findingjobids, _label="GENERATE ITEM DETAIL CALLS")
    
    return itemdetailjobid


def build_bestmatchonly_index(**kwargs):
    sellerdata = kwargs.get('sellerdata')
    configuration = kwargs.get('configuration')
    etlconfiguration = kwargs.get('etlconfiguration', False)
    itemids = kwargs.get('itemids', [])
    sellerdata['etl'] = json.loads(sellerdata['etl'])
    
    if not configuration:
        from etl.config.drivers.picloud.ebay import picloud_ebay_configuration
        configuration = picloud_ebay_configuration

    index = BuildLogic(sellerdata, configuration, PiCloudEBayETLDriver(), etlconfiguration)

    itemidlist = get_sellers_items(index.selleridkey)
    
    itemdetailjobid = cloud.call(generate_items_analytics_main_job, index=index, itemidlist=itemidlist, _type="c1", _label="GENERATE ITEM BESTMATCH ONLY CALLS")
    
    return itemdetailjobid

