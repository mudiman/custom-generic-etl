import os
import MySQLdb
import sys

import time
from decimal import Decimal

import json
from httplib2 import Http
import urllib

try:
    import etl
except ImportError:
    
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
    
from etl.config.loadconfig import COMPANY_CONFIG
from etl.config.loader.mysql import Connection
from etl.system.system import initialize
from etl.dal import insert_stat_snaphot
from etl.dal.solr import get_solr_count, get_solr_querytime
from etl.config import logger, config
    
def solr_index_size_kb(environment="DEVELOPMENT"):
    addr = os.path.join(str(COMPANY_CONFIG['solr'][environment]['path']), "data/index")
    print addr
    size = os.popen('du -b %s' % addr).read()
    res = size.split('\t')[0]
    res = float(res) / 1024
    return res



def avg_item_solr_index_size(environment="DEVELOPMENT"):
    items = get_total_solr_items(environment=environment)
    solrsize = solr_index_size_kb(environment=environment)
    return solrsize / items


def avg_seller_solr_index_size(environment="DEVELOPMENT"):
    sellers = get_total_sellers_in_solr(environment=environment)
    solrsize = solr_index_size_kb(environment=environment)
    return solrsize / sellers

def get_total_sellers_in_solr(environment="DEVELOPMENT"):
    urlparam = "&group=true&group.field=sellerId&group.limit=1&group.format=simple&group.ngroups=true"
    try:
        http = Http()
        
        url = config.SOLR_BASE_URL + "/select?rows=5&indent=true&wt=json&stats=true&q=*:*" + urlparam
        print url
        response, rawBody = http.request(uri=url)
        data = json.loads(rawBody);
        return data['grouped']['sellerId']['ngroups']
    except Exception, e:
        logger.exception(e)


def avg_item_size_solr_index_plus_db(db="company_service_development", environment="DEVELOPMENT"):
    items = get_total_items()
    solrsize = solr_index_size_kb(environment=environment)
    dbitemsize = total_item_db_size_kb(db)
    return (dbitemsize + solrsize) / items

def get_total_sellers():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select count(*) cnt from sellers"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['cnt']

def avg_sellers_per_user():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select sum(a.accounts) cnt from (select users_username,count(*) accounts from users_has_sellers group by users_username) a"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    totalusers = get_total_users()
    return results['cnt'] / totalusers

def get_total_items():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select count(*) cnt from items"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['cnt']

def get_total_solr_items(environment="DEVELOPMENT"):
    
    return get_solr_count(environment=environment)


def get_total_users():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select count(*) cnt from users"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['cnt']

def get_avg_item_db_size(db="company_service_development"):
    totalsize = total_item_db_size_kb(db=db)
    items = get_total_items()
    return totalsize / items
    
def total_item_db_size_kb(db="company_service_development"):
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = """SELECT round(((data_length + index_length) / 1024 ),2) "Size in KB"
    FROM information_schema.TABLES WHERE table_schema = "%s" and TABLE_NAME='items' """ % db
    print query
    cursor.execute(query)
    results = cursor.fetchall()
    totalsize = 0
    for table in list(results):
        totalsize += (table['Size in KB'])
    return totalsize

def get_avg_db_size_per_seller_in_kb():
    totalsellers = get_total_sellers()
    totaldbsize = get_database_size_in_mb() * 1024
    return totaldbsize / totalsellers

def get_avg_db_size_per_user_in_kb():
    totalusers = get_total_users()
    totaldbsize = get_database_size_in_mb() * 1024
    return totaldbsize / totalusers

def get_total_etl_time():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select sum(TIME_TO_SEC(timediff(if(lastupdated='0000-00-00 00:00:00',indexstarttime,lastupdated),indexstarttime))) sec from sellers"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['sec']


def get_total_solr_indexing_time():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select sum(TIME_TO_SEC(timediff(if(solrindexingendtime='0000-00-00 00:00:00',solrindexingstarttime,solrindexingendtime),solrindexingstarttime))) sec from sellers"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['sec']
  
def get_total_scoring_time():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select sum(TIME_TO_SEC(timediff(if(scoringendtime='0000-00-00 00:00:00',scoringstarttime,scoringendtime),scoringstarttime))) sec from sellers"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['sec']


def get_total_suggesstion_time():
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = "select sum(TIME_TO_SEC(timediff(if(suggesstionendtime='0000-00-00 00:00:00',suggesstionstarttime,suggesstionendtime),suggesstionstarttime))) sec from sellers"
    print query
    cursor.execute(query)
    results = cursor.fetchone()
    return results['sec']


def get_database_size_in_mb(db="company_service_development"):
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    query = """SELECT TABLE_NAME, table_rows, data_length, index_length, 
    round(((data_length + index_length) / 1024 / 1024),2) "Size in MB"
    FROM information_schema.TABLES WHERE table_schema = "%s" """ % db
    print query
    cursor.execute(query)
    results = cursor.fetchall()
    totalsize = 0
    for table in list(results):
        totalsize += (table['Size in MB'])
    return float(totalsize)

def avg_indexing_time_per_seller():
    min = get_total_solr_indexing_time()
    sellers = get_total_sellers()
    return min / sellers

def avg_item_per_seller():
    totalitems = get_total_items()
    totalsellers = get_total_sellers()
    return totalitems / totalsellers

def queries_time():
    queriesresponses = {}
    query1 = """select * from categories where sellers_sellerid="officeshoes@GB";""";
    queriesresponses['querytime_getallcategories'] = get_avg_query_time(query1)
    
    
    query1 = """select count(*) as result_count from categories where sellers_sellerid="officeshoes@GB";""";
    queriesresponses['querytime_getcategories_count'] = get_avg_query_time(query1)
    
    query1 = """select ifnull(sum(count),0) as result_count,ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(clickthrough),0) as clickthrough from categories where sellers_sellerid="officeshoes@GB" and categories_categoryid is null;""";
    queriesresponses['querytime_bubbleup_aggregate_to_parent_categories'] = get_avg_query_time(query1)
    
    query1 = """select * from categories, users_has_sellers where categories.sellers_sellerid=users_has_sellers.sellers_sellerid and users_has_sellers.users_username ="faisal";""";
    queriesresponses['querytime_get_users_categories'] = get_avg_query_time(query1)
    
    query1 = """select name,categoryid,sellers_sellerid,categories_categoryid from categories where sellers_sellerid="officeshoes@GB" and categoryid="11450";""";
    queriesresponses['querytime_get_category_details'] = get_avg_query_time(query1)
    
    query1 = """select categoryid from categories where sellers_sellerid="officeshoes@GB" and isleaf=1;""";
    queriesresponses['querytime_get_leaf_categoriesid'] = get_avg_query_time(query1)
    
    query1 = """select categoryid from categories where sellers_sellerid="officeshoes@GB" and categories_categoryid is null;""";
    queriesresponses['querytime_get_parent_categoriesid'] = get_avg_query_time(query1)
    
    query1 = """select * from suggestions where category_id IN (45333) and seller_id="officeshoes@GB";""";
    queriesresponses['querytime_get_category_suggesstions'] = get_avg_query_time(query1)
    
    query1 = """select GROUP_CONCAT(total_score) as total_score,GROUP_CONCAT(image_score) as image_score,
GROUP_CONCAT(title_score) as title_score,GROUP_CONCAT(core_score) as core_score,
GROUP_CONCAT(recommended_score ) as recommended_score ,
date(score_time) as score_time from items where categories_sellers_sellerid ='officeshoes@GB'
and categories_categoryid = 45333  
and locale ='GB' and date(score_time) !=date('0000-00-00') GROUP BY score_time ORDER BY score_time desc;""";
    queriesresponses['querytime_all_scores_of_category'] = get_avg_query_time(query1)
    
    query1 = """select items.* from items,users_has_sellers where items.categories_sellers_sellerid=users_has_sellers.sellers_sellerid and users_has_sellers.users_username ="faisal";""";
    queriesresponses['querytime_get_items_for_users'] = get_avg_query_time(query1)
    
    
    query1 = """select sellerid,storename as storename,if(ifnull(token,0)=0,0,1) as token from sellers, users where username="faisal" and sellerid="officeshoes@GB";""";
    queriesresponses['querytime_single_seller_detail_of_user'] = get_avg_query_time(query1)
    
    query1 = """select sellerid,locale,storename,url,lastupdated,inventory_value,sales,conversionrate,clickthrough,total_score,core_score,recommended_score,result_count from sellers,users_has_sellers where sellerid=users_has_sellers.sellers_sellerid and users_has_sellers.users_username ="faisal";""";
    queriesresponses['querytime_all_seller_detail_of_user'] = get_avg_query_time(query1)
    
    query1 = """select * from items where itemid = 130531581402 and categories_sellers_sellerid="officeshoes@GB";""";
    queriesresponses['querytime_single_itemdetail_for_a_seller'] = get_avg_query_time(query1)
    
    query1 = """select * from items where categories_sellers_sellerid="officeshoes@GB" and categories_categoryid=45333;""";
    queriesresponses['querytime_all_itemdetail_for_a_category_for_a_seller'] = get_avg_query_time(query1)
    
    query1 = """select * from items where categories_sellers_sellerid ="officeshoes@GB";""";
    queriesresponses['querytime_sellers_all_items'] = get_avg_query_time(query1)
    
    
    query1 = """select ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(viewitemperimpression),0) as viewitemperimpression from items where categories_sellers_sellerid="officeshoes@GB" and categories_categoryid=45333;""";
    queriesresponses['querytime_bubbleup_aggregate_to_a_category'] = get_avg_query_time(query1)
    
    query1 = """select sum(variationcount) as result_count from items where categories_sellers_sellerid="officeshoes@GB" and categories_categoryid=45333;""";
    queriesresponses['querytime_get_particular_category_variationcounts'] = get_avg_query_time(query1)
    
    query1 = """select count(*) as result_count from items,users_has_sellers where items.categories_sellers_sellerid=users_has_sellers.sellers_sellerid and users_has_sellers.users_username ="faisal";""";
    queriesresponses['querytime_get_user_itemscount'] = get_avg_query_time(query1)
    
    query1 = """select ifnull(sum(inventory_value),0) as inventory_value,ifnull(sum(sales),0) as sales,ifnull(avg(conversionrate),0) as conversionrate,ifnull(avg(viewItemPerImpression),0) as clickthrough,ifnull(sum(variationcount),0) as result_count from items where categories_sellers_sellerid = 'officeshoes@GB';""";
    queriesresponses['querytime_aggregate_sellerdata'] = get_avg_query_time(query1)
    
    return queriesresponses

def get_avg_query_time(query):
    
    conn = Connection()
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    print query
    startime = time.time()
    cursor.execute(query)
    endtime = time.time()
    difftime = endtime - startime
        
    
    return Decimal(difftime)
    

def get_all_solr_querytime():
    res = {}
    facetfield = "facet=true&"
    res['solrquerytime_sellerid_datascore'] = get_solr_querytime("sellerId:officeshoes@GB")
    res['solrquerytime_sellerid_categoryid_datascore'] = get_solr_querytime("sellerId:officeshoes@GB AND categoryId:11498")
    res['solrquerytime_sellerid_categoryid_a_coreaspects_datascore'] = get_solr_querytime(q="sellerId:officeshoes@GB AND categoryId:11498", facetfield=facetfield + "facet.field=var_Main%20Colour")
    res['solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore'] = get_solr_querytime(q="sellerId:officeshoes@GB AND categoryId:11498", facetfield=facetfield + "facet.field=Main%20Colour&facet.field=var_Main%20Colour&facet.field=Brand")
    res['solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter'] = get_solr_querytime(q="sellerId:officeshoes@GB AND categoryId:11498 AND totalScore:[50 TO 75]", facetfield=facetfield + "facet.field=Main%20Colour&facet.field=var_Main%20Colour&facet.field=Brand")
    
    return res
    
def save_snapshot(db="company_service_development", environment="DEVELOPMENT"):
    total_sellers = get_total_sellers()
    total_items = get_total_items()
    total_users = get_total_users()
    var_avg_item_per_seller = avg_item_per_seller()
    avg_sellers_per_users = avg_sellers_per_user()
    avg_db_size_per_item = get_avg_item_db_size(db=db)
    total_db_size = get_database_size_in_mb() * 1024
    avg_db_per_seller = get_avg_db_size_per_seller_in_kb()
    avg_db_per_user = get_avg_db_size_per_user_in_kb()
    total_solrindex_size = solr_index_size_kb(environment=environment)
    avg_solrindex_per_seller = avg_seller_solr_index_size(environment=environment)
    total_solrindex_item = get_total_solr_items(environment=environment)
    solrindex_to_db_size_ratio = total_solrindex_size / (total_db_size)
    total_size_db_solrindex = total_solrindex_size + (total_db_size)
    total_etl_time = get_total_etl_time()
    total_scoring_time = get_total_scoring_time()
    total_suggesstion_time = get_total_suggesstion_time()
    total_solrindexing_time = get_total_solr_indexing_time()
    
    total_time = total_etl_time + total_scoring_time + total_suggesstion_time
    
    solr_total_sellers = get_total_sellers_in_solr()
    avg_solrindextime_per_user = total_solrindexing_time / total_users
    avg_solrindextime_per_item = total_solrindexing_time / total_items
    avg_solrindextime_per_seller = total_solrindexing_time / total_sellers
    
    avg_suggestiontime_per_seller = total_suggesstion_time / total_sellers
    avg_suggestiontime_per_item = total_suggesstion_time / total_items
    avg_suggestiontime_per_user = total_suggesstion_time / total_users
    
    
    avg_etltime_per_seller = total_etl_time / total_sellers
    avg_etltime_per_user = total_etl_time / total_users
    avg_etltime_per_item = total_etl_time / total_items
    
    avg_scoringtime_per_user = total_scoring_time / total_users
    avg_scoringtime_per_item = total_scoring_time / total_items
    avg_scoringtime_per_seller = total_scoring_time / total_sellers
    
    avg_totaltime_per_item = total_time / total_items
    avg_totaltime_per_user = total_time / total_users
    avg_totaltime_per_seller = total_time / total_sellers
    
    avg_size_db_solrindex_per_seller = total_size_db_solrindex / total_sellers
    avg_size_db_solrindex_per_user = total_size_db_solrindex / total_users
    avg_size_db_solrindex_per_item = total_size_db_solrindex / total_items
    
    queryresponse = queries_time()
    
    querytime_get_leaf_categoriesid = queryresponse['querytime_get_leaf_categoriesid']
    querytime_getallcategories = queryresponse['querytime_getallcategories']
    querytime_bubbleup_aggregate_to_a_category = queryresponse['querytime_bubbleup_aggregate_to_a_category']
    querytime_all_itemdetail_for_a_category_for_a_seller = queryresponse['querytime_all_itemdetail_for_a_category_for_a_seller']
    querytime_aggregate_sellerdata = queryresponse['querytime_aggregate_sellerdata']
    querytime_getcategories_count = queryresponse['querytime_getcategories_count']
    querytime_get_category_details = queryresponse['querytime_get_category_details']
    querytime_get_category_suggesstions = queryresponse['querytime_get_category_suggesstions']
    querytime_get_user_itemscount = queryresponse['querytime_get_user_itemscount']
    querytime_get_users_categories = queryresponse['querytime_get_users_categories']
    querytime_all_seller_detail_of_user = queryresponse['querytime_all_seller_detail_of_user']
    querytime_bubbleup_aggregate_to_parent_categories = queryresponse['querytime_bubbleup_aggregate_to_parent_categories']
    querytime_single_itemdetail_for_a_seller = queryresponse['querytime_single_itemdetail_for_a_seller']
    querytime_all_scores_of_category = queryresponse['querytime_all_scores_of_category']
    querytime_get_parent_categoriesid = queryresponse['querytime_get_parent_categoriesid']
    querytime_get_items_for_users = queryresponse['querytime_get_items_for_users']
    querytime_sellers_all_items = queryresponse['querytime_sellers_all_items']
    querytime_get_particular_category_variationcounts = queryresponse['querytime_get_particular_category_variationcounts']
    querytime_single_seller_detail_of_user = queryresponse['querytime_single_seller_detail_of_user']
    
    solrqueriestime = get_all_solr_querytime()
    solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter = solrqueriestime['solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter']
    solrquerytime_sellerid_datascore = solrqueriestime['solrquerytime_sellerid_datascore']
    solrquerytime_sellerid_categoryid_datascore = solrqueriestime['solrquerytime_sellerid_categoryid_datascore']
    solrquerytime_sellerid_categoryid_a_coreaspects_datascore = solrqueriestime['solrquerytime_sellerid_categoryid_a_coreaspects_datascore']
    solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore = solrqueriestime['solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore']
    
    insert_stat_snaphot(total_sellers, total_items, total_users, var_avg_item_per_seller, avg_sellers_per_users, avg_db_size_per_item, total_db_size, avg_db_per_seller, avg_db_per_user, total_solrindex_size, avg_solrindex_per_seller, total_solrindex_item, solrindex_to_db_size_ratio, total_size_db_solrindex, total_etl_time, total_scoring_time, avg_suggestiontime_per_seller, avg_solrindextime_per_user, avg_solrindextime_per_item, avg_suggestiontime_per_item, avg_suggestiontime_per_user, avg_totaltime_per_item, avg_totaltime_per_user, avg_totaltime_per_seller, avg_solrindextime_per_seller, avg_scoringtime_per_user, avg_etltime_per_seller, total_time, total_suggesstion_time, avg_etltime_per_user, avg_etltime_per_item, avg_scoringtime_per_item, avg_scoringtime_per_seller, total_solrindexing_time, avg_size_db_solrindex_per_seller, avg_size_db_solrindex_per_user, avg_size_db_solrindex_per_item, querytime_get_leaf_categoriesid, querytime_getallcategories, querytime_bubbleup_aggregate_to_a_category, querytime_all_itemdetail_for_a_category_for_a_seller, querytime_aggregate_sellerdata, querytime_getcategories_count, querytime_get_category_details, querytime_get_category_suggesstions, querytime_get_user_itemscount, querytime_get_users_categories, querytime_all_seller_detail_of_user, querytime_bubbleup_aggregate_to_parent_categories, querytime_single_itemdetail_for_a_seller, querytime_all_scores_of_category, querytime_get_parent_categoriesid, querytime_get_items_for_users, querytime_sellers_all_items, querytime_get_particular_category_variationcounts, querytime_single_seller_detail_of_user, solrquerytime_sellerid_categoryid_multiple_coreaspects_dsfilter, solrquerytime_sellerid_datascore, solrquerytime_sellerid_categoryid_datascore, solrquerytime_sellerid_categoryid_a_coreaspects_datascore, solrquerytime_sellerid_categoryid_multiple_coreaspects_datascore, solr_total_sellers)
    
    
if __name__ == "__main__":
    initialize("DEVELOPMENT")
    print "solr index size " + str(get_avg_item_db_size(db="company_service_development"))
    sys.exit();
#    print "avg solr single item index size "+str(avg_item_solr_index_size())
#    print "avg_item_size_solr_index_plus_db "+str(avg_item_size_solr_index_plus_db())
#    print "total sellers "+str(get_total_sellers())
#    print "total items "+str(get_total_items())
#    print "total users "+str(get_total_users())
#    print "db size "+str(get_database_size_in_mb())
#    print "avg item size "+str(get_avg_item_db_size())
#    print "total etl time in min "+str(get_total_etl_time())
#    print "total indexing time in min "+str(get_total_solr_indexing_time())
#    print "total scoring time in min "+str(get_total_scoring_time())
#    print "total suggesstion time in min "+str(get_total_suggesstion_time())
#    print "avg seller indexing time in min "+str(avg_indexing_time_per_seller())
    
    #print solr_index_size()
    #print queries_time()
    #print save_snapshot()
    #print get_all_solr_querytime()
    print get_total_sellers_in_solr()
    Connection("DEVELOPMENT").mysql_connection.close()
