'''
Created on Apr 24, 2012

@author: mudassar
'''
import cloud
from etl.drivers.picloud.ebay import PiCloudEBayETLDriver
from etl.loaders.dumps import make_file_name
import etl
from esync.dumps import get_dump_location
import json
import os
from etl.config import logger
from etl.config.drivers.picloud import setcloudkey


def get_updated_itemids_list(date=None):
    dumplocation = get_dump_location(date=date, type="notifications")
    with open(os.path.join(dumplocation, "filtersummary.json"), 'r') as f:
        data = f.read()
    data = str(data.replace("'", "\""))
    data = json.loads(data)
    itemidlst = []
    for sellerid, categories in data.items():
        for categoryid, itemupdateslst in categories.items():
            itemidlst += itemupdateslst.get('ItemListed', [])
            itemidlst += itemupdateslst.get('ItemRevised', [])
            itemidlst += itemupdateslst.get('ItemSold', [])
    return itemidlst


def get_sold_itemids_list(date=None):
    dumplocation = get_dump_location(date=date, type="notifications")
    with open(os.path.join(dumplocation, "filtersummary.json"), 'r') as f:
        data = f.read()
    data = str(data.replace("'", "\""))
    data = json.loads(data)
    itemidlst = []
    for sellerid, categories in data.items():
        for categoryid, itemupdateslst in categories.items():
            itemidlst += itemupdateslst.get('ItemSold', [])
    return itemidlst


def get_updated_itemids_list_of_seller(date=None, reqsellerid=""):
    dumplocation = get_dump_location(date=date, type="notifications")
    with open(os.path.join(dumplocation, "filtersummary.json"), 'r') as f:
        data = f.read()
    data = str(data.replace("'", "\""))
    data = json.loads(data)
    itemidlst = []
    for sellerid, categories in data.items():
        if sellerid == reqsellerid:
            for categoryid, itemupdateslst in categories.items():
                itemidlst += itemupdateslst.get('ItemListed', [])
                itemidlst += itemupdateslst.get('ItemRevised', [])
    return itemidlst


def get_deleted_itemids_list(date=None):
    dumplocation = get_dump_location(date=date, type="notifications")
    with open(os.path.join(dumplocation, "filtersummary.json"), 'r') as f:
        data = f.read()
    data = str(data.replace("'", "\""))
    data = json.loads(data)
    itemidlst = []
    for sellerid, categories in data.items():
        for categoryid, itemupdateslst in categories.items():
            itemidlst += itemupdateslst.get('ItemClosed', [])
    return itemidlst


def do_bestmatch_call(itemidlistlist=[], callback=None):
    setcloudkey()
    etldriver = PiCloudEBayETLDriver()
    filenamelist = []
    try:
        jobids = []
        for sublist in itemidlistlist:
            jobid = cloud.call(etl.get_item_analytics, etldriver=etldriver, itemid=sublist, _type="c1", _label="GET NOTIFICATION ITEM BESTMATCH DETAILS")
            jobids.append(jobid)
            filename = make_file_name(itemid=sublist, datatype="bestmatch")
            filenamelist.append(filename)
        print jobids
        if len(jobids) > 0:
            cloud.result(jobids, ignore_errors=False)
    except Exception, e:
        print e
    finally:
        return filenamelist
    
    
def do_network_call(itemidlistlist=[], callback=None):
    setcloudkey()
    etldriver = PiCloudEBayETLDriver()
    filenamelist = []
    try:
        jobids = []
        for sublist in itemidlistlist:
            jobid = cloud.call(etl.get_item_details, etldriver=etldriver, itemid=sublist, _type="c1", _label="GET NOTIFICATION ITEM DETAILS")
            jobids.append(jobid)
            filename = make_file_name(itemid=sublist, datatype="itemdetail")
            filenamelist.append(filename)
        print jobids
        if len(jobids) > 0:
            cloud.result(jobids, ignore_errors=False)
    except Exception, e:
        print e
    finally:
        return filenamelist


def download_data_dumps(date="", filenamelist=[]):
    try:
        dumplocation = get_dump_location(date=date, type="data")
        for filename in filenamelist:
            try:
                if cloud.files.exists(filename):
                    cloud.files.get(filename, os.path.join(dumplocation, filename))
                    cloud.files.delete(filename)
                else:
                    print "Not found"
            except Exception, e:
                logger.exception(str(e))
    except Exception, e:
        logger.exception(str(e))

if __name__ == '__main__':
    print get_updated_itemids_list(date="2012-04-26")
    print get_deleted_itemids_list(date="2012-04-26")
    do_network_call(itemidlistlist=[u'300689206906', u'110852167621', u'110852174124', u'110852174124', u'110852207081', u'300668816498', u'300652893303', u'290659710248', u'110818025876', u'110830302090', u'300658444504'], callback=None)
    download_data_dumps(date="2012-04-26", filenamelist=["file____300689206906___itemdetail.txt"])
