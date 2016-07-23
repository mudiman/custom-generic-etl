'''
Created on Oct 26, 2011

@author: Mudassar Ali
'''
import etl
from etl.drivers.threadimp.ebay import ThreadedEBayETLDriver
from etl.loaders.dumps import make_file_name
from etl.config import logger, BESTMATCHONLY
from etl.lib.ThreadPool.ThreadPool import ThreadPool
from etl.miscfunctions import print_progress, getvalue_from_list_with_key
import json
from etl.dal import get_sellers_items
from etl.config.drivers import EBAY_COUNT_LIMIT

class BuildLogic(object):
    '''
    classdocs
    '''


    def __init__(self, sellerdata, configuration, etldriver, etlconfiguration):
        '''
        Constructor
        '''
        self.sellerid = sellerdata.get('ebaysellerid')
        self.selleridkey = sellerdata.get('sellerid')
        self.locale = sellerdata.get('locale')
        self.token = sellerdata.get('token')
        self.etl = sellerdata.get('etl', {})
        self.etlconfiguration = etlconfiguration
        self.filenamelist = []
        self.item_detail_job_list = []
        self.getitem_job_ids = []
        self.etldriver = etldriver
        self.configuration = configuration
        
        self.threadpool = ThreadPool(18)
        
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
            print e
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
                logger.exception(str(e))
        return [writerlist, leaflist]

    
    @print_progress(phase="GETCATEGORIES", progress=None, local=True)
    def get_leaf_categories(self):
        """
            Returns all the leaf level categories in form of dictionary.
            [{'11498': 4569}]
        """
        writelist, leaflist = self.build_category_tree()
        #print writelist
        self.etldriver.loader.writedata(sellerid=self.sellerid, locale=self.locale, datatype='categories', data=writelist)
        return leaflist
    
    @print_progress(phase="GET_ITEMS", progress=None, local=True)
    def get_items(self, leafcategories):
        """
            Starts jobs of geting item of all leaf categories
        """
        #counter = 2
        for leafcategory in leafcategories:
            try:
                #counter -= 1
                leafcategoryid = leafcategory.keys()[0]
                count = leafcategory[leafcategoryid]
                if count > EBAY_COUNT_LIMIT:
                    print str(leafcategoryid) + " category item count is greater then 100000"
                    count = EBAY_COUNT_LIMIT
                pages = count / self.configuration['get_item_calls_per_machine']
                if (pages % self.configuration['get_item_calls_per_machine']) > 0 :pages += 1;
                i = 0
                while True:
                    offset = i * self.configuration['get_item_calls_per_machine']
                    limit = count
                    if pages > 0:
                        limit = self.configuration['get_item_calls_per_machine'] * (i + 1)
                        if limit > count:limit = count
                    logger.info("Geting Getitems leaf categories with id %s with count %s and page %s" % (str(leafcategoryid), str(limit), str(offset)))
                    self.start_thread_job(locals(), 'item')
                    i += 1
                    if i >= pages:break
                #if counter == 0:break
            except Exception, e:
                logger.exception(str(e))


    def callbackfunc(self, itemidlist):
        self.get_item_all_detail(itemidlist)
        

    
    def get_item_all_detail(self, itemids):
        """
            Gets item itemspecifics and best match data
        """
        try:
            self.get_itemsdetail_data(itemids)
            if self.token:
                self.get_bestmatch_data(itemids)
        except Exception, e:
            logger.exception(str(e))
    
        
    
    def get_itemsdetail_data(self, itemids):
        """
            Gets item itemspecifics data
        """
        getitemdetaillist = [itemids[i:i + self.configuration['item_detail_calls_per_machine']] for i in range(0, len(itemids), self.configuration['item_detail_calls_per_machine'])]
        for sublist in getitemdetaillist:
            self.start_thread_job(locals(), 'itemdetail')
    
    def get_bestmatch_data(self, itemids):
        """
            Gets item best match data
        """
        getitembestmatchlist = [itemids[i:i + self.configuration['item_analytics_calls_per_machine']] for i in range(0, len(itemids), self.configuration['item_analytics_calls_per_machine'])]
        for sublist in getitembestmatchlist:
            self.start_thread_job(locals(), 'bestmatch')


    def start_thread_job(self, local, datatype):
        try:
            filename = ""
            if datatype == "bestmatch":
                self.threadpool.add_task(etl.get_item_analytics, etldriver=self.etldriver, itemid=local['sublist'], sellerid=self.selleridkey, locale=self.locale, token=self.token, _label="GET ITEMS BESTMATCH")
                filename = make_file_name(sellerid=self.selleridkey, itemid=local['sublist'], locale=self.locale, datatype=datatype)
            elif datatype == "itemdetail":
                self.threadpool.add_task(etl.get_item_details, etldriver=self.etldriver, itemid=local['sublist'], selleridkey=self.selleridkey, _label="GET ITEMS DETAILS")
                filename = make_file_name(itemid=local['sublist'], datatype=datatype)
            elif datatype == "item":
                self.threadpool.add_task(etl.get_items, etldriver=self.etldriver, sellerid=self.sellerid, selleridkey=self.selleridkey, locale=self.locale, categoryid=local['leafcategoryid'], limit=local['limit'], offset=local['offset'], callback=self.callbackfunc, _label="GET ITEMS")
                filename = make_file_name(sellerid=self.sellerid, selleridkey=self.selleridkey, locale=self.locale, categoryid=local['leafcategoryid'], limit=local['limit'], offset=local['offset'], datatype=datatype)
            self.filenamelist.append(filename)
        except Exception, e:
            logger.exception(str(e))
            
    
    @print_progress(phase="GET_ITEM_ANALYTICS", progress=None, local=True)
    def fetchbestmatchonly(self, sellerid, locale):
        try:
            itemids = get_sellers_items(sellerid + "@" + locale)
            self.get_bestmatch_data(itemids)
        except Exception, e:
            logger.exception(str(e))


    @print_progress(phase="WAIT_FOR_ITEM_DETAIL_AND_ANALYTICS_JOBS", progress=None, local=True)
    def wait_for_item_detail_and_analytics_jobs(self):
        self.threadpool.wait_completion()
        
    @print_progress(phase="INDEXING", progress=None, local=True)
    def start_index(self):
        try:
            if self.etlconfiguration == "ALL":
                self.filenamelist.append(make_file_name(sellerid=self.sellerid, locale=self.locale, datatype='categories'))
                leafcategories = self.get_leaf_categories()
                if "categories" in self.etl.keys():
                    requiredcategories = self.etl['categories']
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
                #leafcategories=[{63869: 1}, {1060: 5}, {53557: 13}, {11504: 2}, {63850: 7}, {24087: 13}, {137084: 3}, {57989: 10}, {155183: 17}, {57988: 23}, {11484: 28}, {11483: 38}, {57990: 44}, {15689: 96}, {15687: 134}]
                #print leafcategories
                self.get_items(leafcategories)
    
            else:
                self.fetchbestmatchonly(self.sellerid, self.locale)
            
            self.wait_for_item_detail_and_analytics_jobs()
            

            return self.filenamelist
        except Exception, e:
            logger.exception(str(e))
            raise e

    

def build_index(**kwargs):
    try:
        sellerdata = kwargs.get('sellerdata')
        configuration = kwargs.get('configuration')
        etlconfiguration = kwargs.get('etlconfiguration', False)
        sellerdata['etl'] = json.loads(sellerdata['etl'])
        
        
        index = BuildLogic(sellerdata, configuration, ThreadedEBayETLDriver(), etlconfiguration)
        return index.start_index()

        
    except Exception, e:
        logger.exception(str(e))
        raise e


        
