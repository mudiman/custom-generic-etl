'''
Created on Apr 23, 2012

@author: mudassar
'''


import etl, os, StringIO, pickle
from elementtree.ElementTree import fromstring
from esync.driver.networkcalls import do_network_call, download_data_dumps, get_updated_itemids_list, \
    get_deleted_itemids_list, get_updated_itemids_list_of_seller, \
    get_sold_itemids_list, do_bestmatch_call
from esync.config import config_picloud_itemdetail_count, \
    config_picloud_bestmatch_count
from esync.driver.loaddata import load_data, delete_items
from etl.dal.solr import updateSolr, delete_items_from_solr
from etl.system.system import initialize, resetwebservers
from etl.config import config, GiveWriteRotatingFileHandler
from esync.dumps import readsummary, readfiltersummary, get_dump_location
from etl.dal import update_categories_inventory, update_sellers_inventory
from esync.driver import callback, print_progress_callback
from esync.driver.authenticreviseditems import save_authentic_revised_items
from esync.driver.inbox_reader import get_body, logout
from esync.driver.item_change import deducting_item
from esync.driver.calculatescore import calculate_score_for
from esync.driver.category_registration import register_categories
from etl.cron.core import etl_scoring_index
from etl.tranformers.ebay.tradingapi import parse_trading_api_get_item
from etl.loaders.dumps import DumpLoader, make_file_name

import logging
from etl.config.loadconfig import COMPANY_CONFIG
from etl.config.drivers import PICLOUDEBAY, THREADEDEBAY

logger = logging.getLogger('itemdetails')
logger.setLevel(logging.NOTSET)

def setloggingforitems():
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rfhandler = GiveWriteRotatingFileHandler(filename=COMPANY_CONFIG['etl']['logs']['service'], mode='a', maxBytes=500000 , backupCount=100)
    rfhandler.setFormatter(formatter)
    rfhandler.setLevel(logging.NOTSET)
    logger.addHandler(rfhandler)

def reading_inbox(**kwargs):
    date = kwargs['date']
    get_body(date)
    logout()
    pass
    

def deducing_items_and_categories(**kwargs):
    date = kwargs['date']
    deducting_item(date)


def save_revised_items(**kwargs):
    date = kwargs['date']    
    sellersItemList = readsummary(date, type="notifications")
    save_authentic_revised_items(date, sellersItemList)


def register_new_categories(**kwargs):    
    date = kwargs['date']    
    sellersItemList = readfiltersummary(date, type="notifications")
    register_categories(date, sellersItemList)

def serialize(listdata):
    try:
        output = StringIO.StringIO()
        temp = pickle.dumps(listdata)
        output.write(temp)
        return output
    except Exception, e:
        print e

def audit_trail():
    pass  
    
def fetch_item_details(**kwargs):
    setloggingforitems()

    p_rel = p_rel = get_dump_location()
    date_str = kwargs['date']
    path = p_rel + "/%s/notifications/" % (str(date_str))
    path_data = p_rel + "/%s/data/" % (str(date_str))
    listing = os.listdir(path)

    itemids = get_updated_itemids_list(date=kwargs['date'])
    for i, item in enumerate(itemids):
        callback(kwargs.get('callback', None), i, len(itemids) - 1)
        logging_item_details(COMPANY_CONFIG['etl']['logs'], item)
        for li in listing:
            if li.find(item) > -1:
                #file reading
                f = open(path + "/" + li, "r")
                #all xml
                text_xml = f.read()
                subs = text_xml.find("<GetItemResponse");
                subs_End = text_xml.find("/GetItemResponse>"); 
                final_xml = '<?xml version="1.0" encoding="UTF-8"?>';  
                final_xml += text_xml[subs:subs_End + 17];
                obj = parse_trading_api_get_item(xml=final_xml)
                filename = make_file_name(datatype="itemdetail", itemid=[item], data=obj)
                pickled_obj = serialize([obj]);
                with open(path_data + "%s" % (filename), "w") as f:
                        f.write(pickled_obj.getvalue())
        #write the parse object to file by pickle it first

    
  

def update_bestmatch_for_whole_seller(**kwargs):
    summary = readsummary(date=kwargs['date'], type="notifications")
    for i, seller in enumerate(summary):
        drivertype = THREADEDEBAY
        etl = False
        bestmatchonly = True
        scoring = False
        index = False
        updatesuggestion = False
        domain = "ebay.company.com"
        etl_scoring_index(seller, drivertype, domain, 'PRODUCTION', etl, bestmatchonly, scoring, index, updatesuggestion, bypassrecentcheck=True)

def update_database(**kwargs):
    delete_items(itemids=get_deleted_itemids_list(date=kwargs['date']))
    load_data(date=kwargs['date'])
    resetwebservers()


def calculate_score(**kwargs):
    date = kwargs['date']    
    sellersItemList = readfiltersummary(date, type="notifications")
    calculate_score_for(sellersItemList)

def bubbleup_data(**kwargs):
    
    summary = readsummary(date=kwargs['date'], type="notifications")
    selleridlist = []
    #bubblescore
    callback(kwargs.get('callback', None), 1, 3)
    for sellerid in summary:
        selleridlist.append(dict(sellerid=sellerid))
    if len(selleridlist) > 0:
        update_categories_inventory(selleridlist)
    callback(kwargs.get('callback', None), 2, 3)
    if len(selleridlist) > 0:
        update_sellers_inventory(selleridlist)
    callback(kwargs.get('callback', None), 3, 3)
    

def reindex_affected_items(**kwargs):
    itemids = get_updated_itemids_list(date=kwargs['date'])
    deleteitemids = get_deleted_itemids_list(date=kwargs['date'])
    allitemslist = itemids + deleteitemids
    #delete items
    getitemdetaillist = [allitemslist[i:i + 20] for i in range(0, len(allitemslist), 20)]
    for suballitemslist in getitemdetaillist:
        itemidquery = "".join([str(i) + " OR " for i in suballitemslist])
        itemidquery = itemidquery[0:len(itemidquery) - 3]
        delete_items_from_solr(itemidquery)
    #update items
    summary = readsummary(date=kwargs['date'], type="notifications")
    for i, seller in enumerate(summary):
        try:
            itemids = get_updated_itemids_list_of_seller(date=kwargs['date'], reqsellerid=seller)
            updateitemdetaillist = [itemids[i:i + 10] for i in range(0, len(itemids), 10)]
            for suballitemslist in updateitemdetaillist:
                itemidquery = "".join([str(i) + "," for i in suballitemslist])
                itemidquery = "itemId in (" + itemidquery[0:len(itemidquery) - 1] + ")"
                extraheaders = dict(condition=itemidquery)
                updateSolr(str(seller), config.ENVIRONMENT, extraheaders)
            callback(kwargs.get('callback', 0), i, len(summary))
        except Exception, e:
            print e


def logging_item_details(path, itemid):
    logger.info('Feting Item with id %s' % (itemid))
    
if __name__ == '__main__':
    initialize("DEVELOPMENT")
#    reindex_affected_items(date="2012-5-7-13")
#    bubbleup_data(date="24-04-2012",callback=print_progress_callback)
#    update_database(date="24-04-2012",callback=print_progress_callback)
    #fetch_item_details(date="test",callback=print_progress_callback)
    update_database(date="2012-8-16-10", callback=print_progress_callback)
