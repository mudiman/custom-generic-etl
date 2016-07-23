'''
Created on May 17, 2012

@author: asimriaz
'''
import MySQLdb
from httplib2 import Http
from elementtree.ElementTree import *
from esync.dumps import readsummary, writedump
from etl.config.loader.mysql import Connection
from etl.config import alllocale
from etl.system.system import initialize
from etl.config.loadconfig import COMPANY_CONFIG

def register_categories(date, sellersItemList):
    """
        This function get a list of sellers and checks all the leaf level categories. If the categories doesnt
        exist in database it will enter the category in database along with its parent
    """
    try:
        for seller in sellersItemList:
            for category in sellersItemList[seller]:
                if  len(sellersItemList[seller][category].get("ItemRevised", [])) > 0 or len(sellersItemList[seller][category].get("ItemListed", [])) > 0:
                    if not category_exists(seller, category):
                        locale = seller.split("@")[1]
                        insert_category_for(seller, category, locale)                 
    except Exception, e:
        print "This exception was caught.Please verify it ", e        
                                                       


def category_exists(sellerid, categoryid):
    """
        This function takes sellerid and categoryid as input and check if it exist in database 
    
    """
    conn = Connection().mysql_connection
    conn.commit()
    cursor = MySQLdb.cursors.DictCursor(conn)
    query = "select * from categories where categoryid = '%s' and  sellers_sellerid='%s'" % (str(categoryid), str(sellerid))
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        return True
    else:
        return False


def insert_category_for(sellerid, category, locale):
    """
        This function takes sellerid and categoryid and locale 
        and it will enter the category in database along with its parent
    
    """
    conn = Connection().mysql_connection
    conn.commit()
    cursor = MySQLdb.cursors.DictCursor(conn)
    headers = {
                         'X-EBAY-API-COMPATIBILITY-LEVEL':'727',
                        'appid':"LMKR5d248-dd5d-49f7-b55f-577916fe70b",
                        'devid':'e4db78d4-dc15-488a-95b2-b29fa6da61a2',
                        'certid':'0a26438a-81ca-47ae-aaf2-b703ca6860ac',
                       'X-EBAY-API-SITEID':str(alllocale[locale]),
                       'X-EBAY-API-CALL-NAME':'GetCategories'
                       }
    payload = '''<?xml version="1.0" encoding="utf-8"?>
    <GetCategoriesRequest xmlns="urn:ebay:apis:eBLBaseComponents">
    <RequesterCredentials>
    <eBayAuthToken>%s</eBayAuthToken>
    </RequesterCredentials>
    <CategorySiteID>%s</CategorySiteID>                
    <LevelLimit>3</LevelLimit>
    <ViewAllNodes>true</ViewAllNodes>
    <DetailLevel>ReturnAll</DetailLevel>
    <WarningLevel>Low</WarningLevel>
    <CategoryParent>%s</CategoryParent>
    </GetCategoriesRequest>''' % (COMPANY_CONFIG["ebaykeys"]["token"], str(alllocale[locale]), str(category))
    
    http = Http()
    responseheader, rawbody = http.request('https://api.ebay.com/ws/api.dll', 'POST', payload, headers)
    responseXml = fromstring(rawbody)
    remove_namespaceNo2(responseXml)            
    category = responseXml.findall('CategoryArray/Category')[0]
    catid = int(category.findtext('CategoryID'))
    name = category.find('CategoryName').text.encode('utf-8')
    parentid = int(category.find('CategoryParentID').text)
    if category.find('LeafCategory') != None:
        leaf_category = 1
    else:
        leaf_category = 0
    if (parentid == catid):
            sqlis = "INSERT INTO categories (sellers_sellerid,categoryid,name,isleaf,) VALUES (%s,%s,%s,%d)  " \
                               % (dbescape(sellerid), dbescape(catid), dbescape(name), leaf_category)
    else:
            if not category_exists(sellerid, parentid):
                insert_category_for(sellerid, str(parentid), locale)
            sqlis = "INSERT INTO categories (sellers_sellerid,categoryid,name,isleaf,categories_categoryid) VALUES (%s,%s,%s,%d,%d)" \
                               % (dbescape(sellerid), dbescape(catid), dbescape(name), leaf_category, parentid)
                
    cursor.execute(sqlis)
    conn.commit()      
                    
def remove_namespaceNo2(doc):
            """Remove namespace in the passed document in place."""
            namespace = 'urn:ebay:apis:eBLBaseComponents'
            ns = u'{%s}' % namespace
            nsl = len(ns)
            for elem in doc.getiterator():
                if elem.tag.startswith(ns):
                    elem.tag = elem.tag[nsl:]
def dbescape(val):
        if val:
            return MySQLdb.string_literal(val)
        else:
            return "NULL"   
        
        
if __name__ == "__main__":
    initialize("DEVELOPMENT")
    date = "2012-05-9-12"
    sellersItemList = readsummary(date, type="notifications")
    register_categories(None, sellersItemList)
