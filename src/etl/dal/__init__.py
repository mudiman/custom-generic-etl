# coding: utf-8

import MySQLdb
import json
from types import DictType
from etl.system.system import initialize
from esync.driver import callback


try:
    import etl
except ImportError:
    import os
    import sys
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from etl.config.loader.mysql import Connection
from etl.config import logger, config, TRIALSELLERDURATION
from etl.config.drivers import SELLER_TOKEN_ENCRYPTION_KEY



def convertsellerlist_to_string(sellers):
    sellerquery = ''
    for seller in sellers: sellerquery += ''.join(""",'%s'""" % (seller['sellerid']))
    sellerquery = sellerquery[1:len(sellerquery)]
    return sellerquery

def dict_unicode_to_str(source):
    newdict = {}
    if not source:
        return None
    for key, value in source.items():
        newdict[str(key)] = str(value)
    return newdict


def get_trail_user_sellerids():
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        query = """select distinct uhs.sellers_sellerid sellerid from users u,users_has_sellers uhs where u.username=uhs.users_username and dateDIFF(CURRENT_TIMESTAMP,u.timestamp)<%d 
and u.package_status="TRAIL" """ % TRIALSELLERDURATION
        #query = """select distinct uhs.sellers_sellerid sellerid from users u,users_has_sellers uhs where u.username=uhs.users_username """        
        #print query
        cursor.execute(query)
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))
        


def get_all_sellers():
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers " % (SELLER_TOKEN_ENCRYPTION_KEY)
        
        #print query
        cursor.execute(query)
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))


def get_all_new_sellers(sellerids=[]):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        sellerscommanstring = convertsellerlist_to_string(sellerids)
        newsellerjson = """{\\"phase\\": \\"NEVER_STARTED\\", \\"progress\\": \\"None\\"}"""
        query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers where status like \"%s\" and sellerid in (%s)" % (SELLER_TOKEN_ENCRYPTION_KEY, newsellerjson, sellerscommanstring)
        
        #print query
        cursor.execute(query)
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))
               
        
def get_all_sellers_for_cron(cronupdate=1):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        #notalreadystarted= ["""{"PHASE": "SCORING", "PROGRESS": "ENDED"}""","""{"PHASE": "SOLRINDEXING", "PROGRESS": "ENDED"}""","""{"phase": "NEVER_STARTED", "progress": "None"}"""]
#        if cronupdate == "all":
#            query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers where status in ('%s','%s','%s') " % (SELLER_TOKEN_ENCRYPTION_KEY,notalreadystarted[0],notalreadystarted[1],notalreadystarted[2])
#        else:
#            query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers where cronupdate=%s and status in ('%s','%s','%s')" % (SELLER_TOKEN_ENCRYPTION_KEY, cronupdate,notalreadystarted[0],notalreadystarted[1],notalreadystarted[2])

        if cronupdate == "all":
            query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers " % (SELLER_TOKEN_ENCRYPTION_KEY)
        else:
            query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers where cronupdate=%s " % (SELLER_TOKEN_ENCRYPTION_KEY, cronupdate)
        #print query
        cursor.execute(query)
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))


def get_all_sellers_unsuccessfull_sellers_to_update(sellers):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        successtag = """{"PHASE": "INDEXING", "PROGRESS": "ERROR"}"""
        startnotend= """{"PHASE": "INDEXING", "PROGRESS": "STARTED"}"""
        tempsellers = []
        if len(sellers) > 0:
            if type(sellers[0]) != DictType:
                for seller in sellers:
                    temp = dict(sellerid=seller)
                    tempsellers.append(temp)
            else:
                tempsellers = sellers
        sellerquery = convertsellerlist_to_string(tempsellers)
        query = "select sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token,status from sellers where sellerid in (%s) and (status='%s' or status='%s') " % (SELLER_TOKEN_ENCRYPTION_KEY,sellerquery, successtag,startnotend)
        #print query
        cursor.execute(query)
        
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))

def get_all_sellers_successfull_sellers_to_update(sellers):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        successtag = """{"PHASE": "INDEXING", "PROGRESS": "ENDED"}"""
        tempsellers = []
        if len(sellers) > 0:
            if type(sellers[0]) != DictType:
                for seller in sellers:
                    temp = dict(sellerid=seller)
                    tempsellers.append(temp)
            else:
                tempsellers = sellers
        sellerquery = convertsellerlist_to_string(tempsellers)
        query = "select sellerid,ebaysellerid,locale from sellers where sellerid in (%s) and status='%s'" % (sellerquery, successtag)
        #print query
        cursor.execute(query)
        
        results = cursor.fetchall()
        res = []
        for result in results:res.append(dict_unicode_to_str(result))
        return res
    except Exception, e:
        logger.exception(str(e))


def get_all_items_with_brand(categoryid=None, sellerid=None):
    try:
        brandspecific='Brand'
        if sellerid.split('@')[1]=="DE":
            brandspecific=u"Marke"
        res = []
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        sql = """select distinct title,properties from items where properties like '%"""+str(brandspecific)+"""%' """
        if categoryid:
            sql = """select distinct title,properties from items where properties like '%"""+str(brandspecific)+"""%' and categories_categoryid=""" + str(categoryid)
        if sellerid:
            sql += " and categories_sellers_sellerid='" + str(sellerid) + "'"
        #print sql
        cursor.execute(sql)
        results = cursor.fetchall()
        
        for result in results:
            temp = {}
            temp['title'] = result['title']
            properties = json.loads(result['properties'])
            if properties.get(brandspecific):
                temp['brand'] = properties[brandspecific]
                res.append(temp)
    except Exception, e:
        logger.exception(str(e))   
    finally:
        return res 
        
        
def get_sellers_items(sellerid):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        cursor.execute("select itemid from items where categories_sellers_sellerid='%s'" % sellerid)
        results = cursor.fetchall()
        res = []
        for result in results:res.append(result['itemid'])
        return res
    except Exception, e:
        logger.exception(str(e))    

def get_seller_by_id(sellerid):
    try:
        conn = Connection()
        cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
        #notalreadystarted= ["""{"PHASE": "SCORING", "PROGRESS": "ENDED"}""","""{"PHASE": "SOLRINDEXING", "PROGRESS": "ENDED"}""","""{"phase": "NEVER_STARTED", "progress": "None"}""","""{"PROGRESS": "ERROR"}"""]
        #query = "select sellerid sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token from sellers where sellerid='%s' and status in ('%s','%s','%s','%s') " % (SELLER_TOKEN_ENCRYPTION_KEY, sellerid,notalreadystarted[0],notalreadystarted[1],notalreadystarted[2],notalreadystarted[3])
        query = "select sellerid sellerid,etl,ebaysellerid,locale,AES_DECRYPT(token,'%s') as token from sellers where sellerid='%s' " % (SELLER_TOKEN_ENCRYPTION_KEY, sellerid)
        cursor.execute(query)
        result = cursor.fetchone()
        return dict_unicode_to_str(result)
    except Exception, e:
        logger.exception(str(e))


def reset_sellerdata(sellers):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        query = "delete from categories where sellers_sellerid in (%s) " % sellerquery
        #print query
        cursor.execute(query)
        query = "delete from items where categories_sellers_sellerid in (%s) " % sellerquery
        #print query
        cursor.execute(query)
        conn.commit()
        cursor.close()

    except Exception, e:
        logger.exception(str(e))



def get_all_leafcategories(sellerid=None):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        if sellerid:
            squery = "select distinct sellers_sellerid sellerid,categoryid,name from  categories where isleaf=1 AND sellers_sellerid='%s'" % sellerid
        else:
            squery = "select distinct sellers_sellerid sellerid,categoryid,name from  categories where isleaf=1"
        #print squery
        cursor.execute(squery)
        results = cursor.fetchall()
        return list(results)
    except Exception, e:
        logger.exception(str(e))


def get_all_leafcategories_categoryidonly(sellerid=None):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        if sellerid:
            squery = "select distinct categoryid,name from  categories where isleaf=1 AND sellers_sellerid='%s' and count>0 order by count desc" % sellerid
        else:
            squery = "select distinct categoryid,name from  categories where isleaf=1 and count>0 order by count desc"
        #print squery
        cursor.execute(squery)
        results = cursor.fetchall()
        return list(results)
    except Exception, e:
        logger.exception(str(e))
        
def get_all_leafcategories_list(locale="GB"):
    leafcategories = get_all_leafcategories()
    temp = []
    for item in leafcategories:
        if item['sellerid'].split("@")[1] == locale:
            temp.append(item['categoryid'])
    
    temp = list(set(temp))
    return temp


def truncatesuggestion(sellerid=None):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        if sellerid:
            squery = "delete from suggestions where seller_id='%s'" % sellerid
        else:
            squery = "truncate table suggestions"
        #print squery
        cursor.execute(squery)
        conn.commit()
    except Exception, e:
        logger.exception(str(e))
    
def insert_suggestion(sellerid, categoryid, property, problem, affectedcount, inventoryvalue, sales):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        squery = "insert into suggestions (`seller_id`,`category_id`,`property`,`problem`,`affected_count`,`inventory_value`,`sales`) "\
        " values ('%s','%s','%s','%s','%s','%s','%s') " % (sellerid, categoryid, property, problem, affectedcount, inventoryvalue, sales)
        #print squery
        cursor.execute(squery)
        conn.commit()
    except Exception, e:
        logger.exception(str(e))
        

def update_sellers_status(sellers, progress):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        squery = "update sellers set status='%s' where sellerid in (%s)" % (json.dumps(progress), sellerquery)
        #print squery
        cursor.execute(squery)
        conn.commit()
    except Exception, e:
        logger.exception(str(e))


def update_sellers_etl_scoring_or_solrindexing_time(sellers, phase, progress):
    try:
        field = ""
        endfield = ""
        if phase == "SCORING":
            field = "scoring" + progress + "time"
            endfield = "scoringendtime"
        if phase == "SOLRINDEXING":
            field = "solrindexing" + progress + "time"
            endfield = "solrindexingendtime"
        if phase == "SUGGESSTIONINDEXING":
            field = "suggesstion" + progress + "time"
            endfield = "suggesstionendtime"
        if phase == "ETL":
            field = "indexstarttime"
            endfield = "lastupdated"
                
        conn = Connection().mysql_connection
        conn.commit()
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        query = "update sellers set " + field + "=CURRENT_TIMESTAMP() where sellerid in (%s)" % (sellerquery)
        #print query
        cursor.execute(query)
        if progress == "start":
            query = "update sellers set " + endfield + "= NULL where sellerid in (%s)" % (sellerquery)
            #print query
            cursor.execute(query)
        conn.commit()
    except Exception, e:
        logger.exception(str(e))

def update_sellers_timestart(sellers):
    try:
        conn = Connection().mysql_connection
        conn.commit()
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        query = "update sellers set indexstarttime=CURRENT_TIMESTAMP() where sellerid in (%s)" % (sellerquery)
        #print query
        cursor.execute(query)

        conn.commit()
    except Exception, e:
        logger.exception(str(e))
                
#def update_sellers_inventory(sellers):
#    try:
#        conn = Connection().mysql_connection
#        conn.commit()
#        cursor = MySQLdb.cursors.DictCursor(conn)
#        sellerquery = convertsellerlist_to_string(sellers)
#        query = "select categories_sellers_sellerid sellerid,ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(viewItemPerImpression),0) as clickthrough,ifnull(sum(variationcount),0) as result_count from items where categories_sellers_sellerid in (%s) group by categories_sellers_sellerid" % sellerquery
#        print query
#        cursor.execute(query)
#        results = cursor.fetchall()
#        for result in results:
#            query = "update sellers set inventory_value=%s,sales=%s,conversionrate=%s,clickthrough=%s,result_count=%s,lastupdated=CURRENT_TIMESTAMP() where sellerid='%s'" % (result['inventory_value'],result['sales'],result['conversionrate'],result['clickthrough'], result['result_count'],result['sellerid'])
#            cursor.execute(query)
#            print query
#        conn.commit()
#    except Exception, e:
#        logger.exception(str(e))

def update_sellers_inventory(sellers):
    try:
        conn = Connection().mysql_connection
        conn.commit()
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        
        for sellers in sellers:
            query = "select ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(viewItemPerImpression),0) as clickthrough,ifnull(sum(variationcount),0) as result_count from items where categories_sellers_sellerid = '%s'" % sellers["sellerid"]
            #print query
            cursor.execute(query)
            results = cursor.fetchall()
            if len(results) > 0:
                result = results[0]
                query = "update sellers set inventory_value=%s,sales=%s,conversionrate=%s,clickthrough=%s,result_count=%s,lastupdated=CURRENT_TIMESTAMP() where sellerid='%s'" % (result['inventory_value'], result['sales'], result['conversionrate'], result['clickthrough'], result['result_count'], sellers["sellerid"])
            else:
                query = "update sellers set inventory_value=%s,sales=%s,conversionrate=%s,clickthrough=%s,result_count=%s,lastupdated=CURRENT_TIMESTAMP() where sellerid='%s'" % (0, 0, 0, 0, 0, sellers["sellerid"])
                
            cursor.execute(query)
        conn.commit()
    except Exception, e:
        logger.exception(str(e))
        
def update_categories_inventory(sellers):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        sellerquery = convertsellerlist_to_string(sellers)
        query = "select sellers_sellerid sellerid,categoryid from categories where sellers_sellerid in (%s) order by sellers_sellerid" % sellerquery
        cursor.execute(query)
        results = cursor.fetchall()
        for i, result in enumerate(results):
            query = "call getItemsOfCategory('%d','%s','ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(viewItemPerImpression),0) as viewItemPerImpression,ifnull(sum(variationcount),0) as cnt','','');" % (result['categoryid'], result['sellerid'])
            cursor.execute(query)
            count = cursor.fetchone()
            cursor.close()
            cursor = MySQLdb.cursors.DictCursor(conn)
            query = "update categories set count=%d,inventory_value=%s,sales=%s,conversionrate=%s,clickthrough=%s where categoryid=%d and sellers_sellerid='%s'" % (count['cnt'], count['inventory_value'], count['sales'], count['conversionrate'], count['viewItemPerImpression'], result['categoryid'], result['sellerid'])
            cursor.execute(query)
        conn.commit()
        cursor.close()

    except Exception, e:
        logger.exception(str(e))
        

def insert_stat_snaphot(total_sellers=0, total_items=0, total_users=0, avg_item_per_seller=0, avg_sellers_per_users=0, avg_db_size_per_item=0, total_db_size=0, avg_db_per_seller=0, avg_db_per_user=0, total_solrindex_size=0, avg_solrindex_per_seller=0, total_solrindex_item=0, solrindex_to_db_size_ratio=0, total_size_db_solrindex=0, total_etl_time=0, total_scoring_time=0, avg_suggestiontime_per_seller=0, avg_solrindextime_per_user=0, avg_solrindextime_per_item=0, avg_suggestiontime_per_item=0, avg_suggestiontime_per_user=0, avg_totaltime_per_item=0, avg_totaltime_per_user=0, avg_totaltime_per_seller=0, avg_solrindextime_per_seller=0, avg_scoringtime_per_user=0, avg_etltime_per_seller=0, total_time=0, total_suggesstion_time=0, avg_etltime_per_user=0, avg_etltime_per_item=0, avg_scoringtime_per_item=0, avg_scoringtime_per_seller=0, total_solrindexing_time=0, avg_size_db_solrindex_per_seller=0, avg_size_db_solrindex_per_user=0, avg_size_db_solrindex_per_item=0, querytime_get_leaf_categoriesid=0, querytime_getallcategories=0, querytime_bubbleup_aggregate_to_a_category=0, querytime_all_itemdetail_for_a_category_for_a_seller=0, querytime_aggregate_sellerdata=0, querytime_getcategories_count=0, querytime_get_category_details=0, querytime_get_category_suggesstions=0, querytime_get_user_itemscount=0, querytime_get_users_categories=0, querytime_all_seller_detail_of_user=0, querytime_bubbleup_aggregate_to_parent_categories=0, querytime_single_itemdetail_for_a_seller=0, querytime_all_scores_of_category=0, querytime_get_parent_categoriesid=0, querytime_get_items_for_users=0, querytime_sellers_all_items=0, querytime_get_particular_category_variationcounts=0, querytime_single_seller_detail_of_user=0, solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter=0, solrquerytime_sellerid_datascore=0, solrquerytime_sellerid_categoryid_datascore=0, solrquerytime_sellerid_categoryid_a_coreaspects_datascore=0, solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore=0, solr_total_sellers=0):
    try:
        conn = Connection().mysql_connection
        cursor = MySQLdb.cursors.DictCursor(conn)
        query = """insert into stats (`total_sellers`,`total_items`,`total_users`,`avg_item_per_seller`,`avg_sellers_per_users`,
        `avg_db_size_per_item`,`total_db_size`,`avg_db_per_seller`,`avg_db_per_user`,`total_solrindex_size`,`avg_solrindex_per_seller`,
        `total_solrindex_item`,`solrindex_to_db_size_ratio`,`total_size_db_solrindex`,`total_etl_time`,`total_scoring_time`,
        `avg_suggestiontime_per_seller`,`avg_solrindextime_per_user`,`avg_solrindextime_per_item`,`avg_suggestiontime_per_item`,
        `avg_suggestiontime_per_user`,`avg_totaltime_per_item`,`avg_totaltime_per_user`,`avg_totaltime_per_seller`,
        `avg_solrindextime_per_seller`,`avg_scoringtime_per_user`,`avg_etltime_per_seller`,`total_time`,`total_suggesstion_time`,
        `avg_etltime_per_user`,`avg_etltime_per_item`,`avg_scoringtime_per_item`,`avg_scoringtime_per_seller`,`total_solrindexing_time`,
        `avg_size_db_solrindex_per_seller`,`avg_size_db_solrindex_per_user`,`avg_size_db_solrindex_per_item`,
        `querytime_get_leaf_categoriesid`,`querytime_getallcategories`,`querytime_bubbleup_aggregate_to_a_category`,
        `querytime_all_itemdetail_for_a_category_for_a_seller`,`querytime_aggregate_sellerdata`,`querytime_getcategories_count`,
        `querytime_get_category_details`,`querytime_get_category_suggesstions`,`querytime_get_user_itemscount`,`querytime_get_users_categories`,
        `querytime_all_seller_detail_of_user`,`querytime_bubbleup_aggregate_to_parent_categories`,`querytime_single_itemdetail_for_a_seller`,
        `querytime_all_scores_of_category`,`querytime_get_parent_categoriesid`,`querytime_get_items_for_users`,`querytime_sellers_all_items`,
        `querytime_get_particular_category_variationcounts`,`querytime_single_seller_detail_of_user`,`solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter`,
        `solrquerytime_sellerid_datascore`,`solrquerytime_sellerid_categoryid_datascore`,`solrquerytime_sellerid_categoryid_a_coreaspects_datascore`,
        `solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore`,`solr_total_sellers`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ % \
        (total_sellers, total_items, total_users, avg_item_per_seller, avg_sellers_per_users, avg_db_size_per_item, total_db_size, avg_db_per_seller, avg_db_per_user, total_solrindex_size, avg_solrindex_per_seller, total_solrindex_item, solrindex_to_db_size_ratio, total_size_db_solrindex, total_etl_time, total_scoring_time, avg_suggestiontime_per_seller, avg_solrindextime_per_user, avg_solrindextime_per_item, avg_suggestiontime_per_item, avg_suggestiontime_per_user, avg_totaltime_per_item, avg_totaltime_per_user, avg_totaltime_per_seller, avg_solrindextime_per_seller, avg_scoringtime_per_user, avg_etltime_per_seller, total_time, total_suggesstion_time, avg_etltime_per_user, avg_etltime_per_item, avg_scoringtime_per_item, avg_scoringtime_per_seller, total_solrindexing_time, avg_size_db_solrindex_per_seller, avg_size_db_solrindex_per_user, avg_size_db_solrindex_per_item, querytime_get_leaf_categoriesid, querytime_getallcategories, querytime_bubbleup_aggregate_to_a_category, querytime_all_itemdetail_for_a_category_for_a_seller, querytime_aggregate_sellerdata, querytime_getcategories_count, querytime_get_category_details, querytime_get_category_suggesstions, querytime_get_user_itemscount, querytime_get_users_categories, querytime_all_seller_detail_of_user, querytime_bubbleup_aggregate_to_parent_categories, querytime_single_itemdetail_for_a_seller, querytime_all_scores_of_category, querytime_get_parent_categoriesid, querytime_get_items_for_users, querytime_sellers_all_items, querytime_get_particular_category_variationcounts, querytime_single_seller_detail_of_user, solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter, solrquerytime_sellerid_datascore, solrquerytime_sellerid_categoryid_datascore, solrquerytime_sellerid_categoryid_a_coreaspects_datascore, solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore, solr_total_sellers);
        
        #print query
        cursor.execute(query)
        conn.commit()
        cursor.close()
    except Exception, e:
        logger.exception(str(e))


def get_leaf_categories(sellerid, categoryid=None):
    try:
        conn = Connection().mysql_connection          
        cursor = MySQLdb.cursors.DictCursor(conn)
        if not categoryid:
            categoryid = "%"
        cursor.execute("SET @@session.max_sp_recursion_depth = 100;")
        query = "call %s('%s','%s',@leaf);" % ('getLeafCategoryIdsOfParentCategory', categoryid, sellerid)
        #print query
        cursor.execute(query);
        cursor.execute("select @leaf");
        results = cursor.fetchall()
        cursor.execute("SET @@session.max_sp_recursion_depth = 0;")
        data = results[0]['@leaf'].split(',')
        return data
    except Exception, e:
        print e
        
        
if __name__ == "__main__":
    Environment = "DEVELOPMENT"
    initialize(Environment)

#    update_categories_inventory([{'sellerid':'buy_united_kingdom@GB'}])
#    update_sellers_inventory([{'sellerid':'buy_united_kingdom@GB'}])
#    temp=dict(PHASE="TEST",PROGRESS="TEST")
#    update_sellers_status([{'sellerid':"thekatsboutique@US"}],temp)
#    print update_sellers_scoring_or_solrindexing_time([{'sellerid':'officeshoes@GB'}], "SCORING", "start")
#    print update_sellers_scoring_or_solrindexing_time([{'sellerid':'officeshoes@GB'}], "SCORING", "end")
    #print get_all_leafcategories()
