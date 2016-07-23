'''
Created on Dec 22, 2011

@author: Mudassar Ali
'''

from httplib2 import Http

import urllib
import os
import json
from urllib2 import quote
from decimal import Decimal
import time
from elementtree.ElementTree import fromstring





try:
    import etl
except ImportError:
    import sys
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from etl.config import config, logger, ETL_DEBUG_PRINT
from etl.dal.sisdal import get_item_count, get_csv, \
    replace_special_char_from_field, replace_special_char_for_solr
from etl.system.system import initialize, restart_tomcat



def solrcallwrapper(url=None, method=None, body=None, headers=None):
    fibonic = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    http = Http()
    for i in range(0, len(fibonic)):
        try:
            if i==8:
                restart_tomcat()
            responseheaders, rawbody = http.request(uri=url, method=method, body=body, headers=headers)
            if str(responseheaders['status']) != '200':
                if ETL_DEBUG_PRINT:
                    print "failed " + str(url)
                    logger.debug("failed " + str(url))
                    logger.critical(rawbody)
                time.sleep(fibonic[i])
                
            else:
                return [responseheaders, rawbody]
        except Exception, e:
            logger.exception(str(e))
            time.sleep(fibonic[i])
    return [None, None]


def updateSolr(sellerid, db='TEST', extraheaders={}):
    status = False
    count = get_item_count(sellerid, db, extraheaders)
    for i in range(0, int(count), 100):
        try:
            csvdata = get_csv(sellerid, str(i), db, extraheaders)
            solrurl = config.SOLR_BASE_URL + """/update/csv?commit=true&keepEmpty=true&trim=true&encapsulator=" """
            response, rawBody = solrcallwrapper(url=str(solrurl), method="POST", body=csvdata, headers=None)
            print rawBody
        except Exception, e:
            logger.exception(e)

    return status


def get_solr_count(sellerid=None, environment="DEVELOPMENT"):
    try:
        if not sellerid:
            url = config.SOLR_BASE_URL + "/select?rows=5&indent=true&wt=json&stats=true&q=*:*" 
        response, rawBody = solrcallwrapper(url=url, method="GET", body=None, headers=None)
        data = json.loads(rawBody);
        return data['response']['numFound']
    except Exception, e:
        logger.exception(e)


def get_solr_query_count(q="*:*", urlparam=""):
    try:
        http = Http()
        
        url = config.SOLR_BASE_URL + "/select?rows=5&indent=true&wt=json&stats=true&q=" + urllib.quote_plus(q) + urlparam
        print url
        response, rawBody = solrcallwrapper(url=url, method="GET", body=None, headers=None)
        data = json.loads(rawBody);
        return data['grouped']['sellerId']['ngroups']
    except Exception, e:
        logger.exception(e)
        
   
def get_solr_querytime(q="*:*", facetfield=""):
    try:
        http = Http()
        url = config.SOLR_BASE_URL + "/select?rows=5&indent=true&wt=json&stats=true&q=" + urllib.quote_plus(q) + facetfield 
        res = []
        for i in range(0, 10):
            response, rawBody = solrcallwrapper(url=url, method="GET", body=None, headers=None)
            data = json.loads(rawBody);
            res.append(float(data['responseHeader']['QTime']) / 1000)
    
        return Decimal(sum(res) / len(res))
    
    except Exception, e:
        logger.exception(e)
      

def delete_items_from_solr(itemids):
    if itemids != "":
        url = config.SOLR_BASE_URL + "/update?commit=true&stream.body=" + urllib.quote_plus("<delete><query>itemId:(%s)</query></delete>" % itemids)
        print url
        response, rawBody = solrcallwrapper(url=url, method="GET", body=None, headers=None)
        print response
        print rawBody
    

def resetSolr(sellerid):
    http = Http()
    url = config.SOLR_BASE_URL + "/update?commit=true&stream.body=" + urllib.quote_plus("<delete><query>sellerId:%s</query></delete>" % sellerid)
    print url
    response, rawBody = solrcallwrapper(url=url, method="GET", body=None, headers=None)
    print response
    print rawBody
    
    


field_list = ["sales", "inventoryValue"]

def getstatsoffields(sellerid, categoryid, problem, field):
    try:
        http = Http()
        tempfield = replace_special_char_for_solr([field])[0]
        url = config.SOLR_BASE_URL + "/select?rows=20&indent=true&wt=json&stats=true&q=" 
        condition = "sellerId:%s AND categoryId:%s AND " % (sellerid, categoryid)
        condition = urllib.quote_plus(condition) + "%s:%s" % (urllib.quote_plus(tempfield), urllib.quote_plus(problem))
        url += condition + "&group=true&group.field=itemId&group.limit=1&group.ngroups=true"
        
        
        response, rawBody = solrcallwrapper(url=url + "&rows=0", method="GET", body=None, headers=None)
        data = json.loads(rawBody)
        
        totalcount = data['grouped']['itemId']['ngroups']
        result = {}
        for fielddd in field_list:
            result[fielddd] = 0
        for i in range(0, totalcount, 20):
            response, rawBody = http.request(uri=url + "&start=" + str(i))
            tempdata = json.loads(rawBody)
            for group in tempdata['grouped']['itemId']['groups']:
                for fielddd in field_list:
                    result[fielddd] += group['doclist']['docs'][0][fielddd]

        
        return result
    except Exception, e:
        logger.exception(e)
        
        
if __name__ == '__main__':
    initialize("DEVELOPMENT")
    
#    resetSolr('officeshoes@GB')
#    updateSolr('officeshoes@GB',"DEVELOPMENT")
    print getstatsoffields('buy_united_kingdom@GB', 174, 'COMPANY-invalid-$*', 'Type')
    

