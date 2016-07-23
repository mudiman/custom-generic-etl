'''
Created on Apr 24, 2012

@author: mudassar
'''
from esync.dumps import get_dump_location
import os
import json
from etl.loaders.mysql.items import import_items
from etl.config.loader.mysql import Connection
import MySQLdb
from etl.config import logger
import pickle
from types import StringType


def generate_data_array(date=""):
    dumplocation = get_dump_location(date=date, type="data")
    listing = os.listdir(dumplocation)
    itemlistlist = [listing[i:i + 100] for i in range(0, len(listing), 100)]
    data = []
    for itemlist in itemlistlist:
        temp = ""
        for item in itemlist:
            
            if item.find('item') > 0:
                with open(os.path.join(dumplocation, item), 'r') as f:
                    temp = f.read()
                temp = pickle.loads(temp)
                for ll in temp:
                    if ll.get('variation'):
                        if type(ll.get('variation')) != StringType:
                            ll['variation'] = json.dumps(ll.get('variation', {}))
                    if ll.get('itemspecifics'):
                        if type(ll.get('itemspecifics')) != StringType:
                            ll['itemspecifics'] = json.dumps(ll.get('itemspecifics', {}))
                if temp != "":
                    data += temp
                if len(data) > 500:
                    yield data
                    data = []
    if len(data) > 0:
        yield data
    else:
        yield None
         
    
def delete_items(itemids=[], callback=None):
    try:
        if len(itemids) > 0:
            itemidquery = "".join([str(i) + "," for i in itemids])
            itemidquery = itemidquery[0:len(itemidquery) - 1]
            conn = Connection().mysql_connection
            conn.commit()
            cursor = MySQLdb.cursors.DictCursor(conn)
            query = "delete from items where itemid in (%s)" % (itemidquery)
            print query
            cursor.execute(query)
            conn.commit()
    except Exception, e:
        logger.exception(str(e))


def record_change(olditem, newitem):
    for key in olditem:
        if olditem.get('currentprice') != newitem.get('currentprice'):
            save_change(olditem['itemid'], key, olditem.get(key), newitem.get(key))
        if olditem.get('totalscore') != newitem.get('totalscore'):
            save_change(olditem['itemid'], key, olditem.get(key), newitem.get(key))
            pass
            
    
def save_change(sellerid, categoryid, itemid, eventname, oldvalue, newvalue):
    conn = Connection().mysql_connection
    cursor = MySQLdb.cursors.DictCursor(conn)
    query = """insert into events (sellerid,locale,categoryid,itemid,event_name,old_value,new_value) values ('%s','%s','%s','%s','%s','%s','%s')""" % \
     (sellerid, sellerid.split('@')[1], categoryid, itemid, eventname, oldvalue, newvalue)
    print query
    cursor.execute(query)
    conn.commit()
    
def load_data(date=""):
    items = generate_data_array(date)
    itemsdata = items.next()
    #print itemsdata
    if itemsdata:
        while True:
            try:
                if len(itemsdata) > 0:
                    import_items(itemsdata)
                itemsdata = items.next()
            except StopIteration:
                return
    
    
if __name__ == '__main__':
    pass
