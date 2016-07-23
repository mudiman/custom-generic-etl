'''
Created on Jun 8, 2012

@author: mudassar
'''
import MySQLdb
import json
from httplib2 import Http
from etl.config import config
from etl.system.system import initialize
from etl.config.loader.mysql import Connection
import sys
from etl.dal import get_leaf_categories
from mining.misc import exactmatch, fingerprint_exactmatch, \
    levenshtein_exactmatch, levenshtein_and_fingerprint_mixed




def get_category_recommended_item_specifics(categoryid, locale):
    try:
        http = Http()
        newheader = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'locale':locale}
        url = config.METADATA_SERVICE_BASE + "/categories/%s/values" % categoryid
        response, rawBody = http.request(uri=url, method="GET", headers=newheader)
        data = json.loads(rawBody)
        res = {}
        for property, value in data.get('data', {}).items():
            res[property] = value
        return res
    except Exception, e:
        print str(e)

comparisonalgo = {"levenstien_distance":levenshtein_exactmatch, "exactmatch":exactmatch, \
                "fingerprint":fingerprint_exactmatch, "levenshtein_and_fingerprint_mixed":levenshtein_and_fingerprint_mixed}
#ignorelist=["Mens","Womens"]
       
def scan_title_for_single_item(title, itemspecific, specifickeyonly=None):
    properties = []
    for key, value in itemspecific.items():
        if specifickeyonly != None and specifickeyonly != str(key):
            continue
        for val in value:
            propertiesfound = {}
            if comparisonmode(title, val, algo):
                propertiesfound[key] = val
                properties.append(propertiesfound)
    return properties


def ngram_tokenizer(title, maxngram):
    ntokens = []
    tokens = title.split(" ")
    newtokens = tokens
#    for token in tokens:
#        if token not in ignorelist:
#            newtokens.append(token)
    for gram in range(1, maxngram + 1):
        temp = get_consecutive_words(newtokens, gram)
        ntokens.append(temp)
    return ntokens

def get_consecutive_words(tokens, wordlength):
    res = []
    for i in range(len(tokens) - wordlength + 1):
        temp = ""
        for j in range(wordlength):
            temp += tokens[i + j] + " "
        temp = temp[0:len(temp) - 1]
        res.append(temp)
    return res
        

def comparision_with_token(tokens, match, algo):
    for gram in range(0, maxngram):
        for token in tokens[gram]:
            res = comparisonalgo[algo](token=token, match=match)
            if res:
                return True
    return False

def comparisonmode(title, reference, algo):
    token = None
    tokens = ngram_tokenizer(title, maxngram)
    if algo == "exactmatch":
        return comparisonalgo[algo](search=title, token=token, match=reference)
    elif algo in ["fingerprint", "levenstien_distance", "levenshtein_and_fingerprint_mixed"]:
        return comparision_with_token(tokens, reference, algo)



def get_seller_categories_item_count(sellerid, categoryid, Environment, extraheaders={}):
    try:
        count = 0
        http = Http()
        url = config.SERVICE_BASE + "/sellers/%s/categories/%s/items" % (sellerid, categoryid)
        headers = {
            'Authorization' : config.ROOT_KEY,
            'Environment'    : Environment
            }
        headers.update(extraheaders)
        response, rawBody = http.request(uri=url, method="GET", headers=headers)
        if response['status'] != "200":
            return count
        count = response.get('totalcount', 0)
        return count
    except Exception, e:
        raise e
    

def seller_items(sellerid, categoryid, Environment, extraheaders={}):
    try:
        count = 0
        http = Http()
        itemcount = get_seller_categories_item_count(sellerid, categoryid, Environment, extraheaders)
        items = []
        url = config.SERVICE_BASE + "/sellers/%s/categories/%s/items" % (sellerid, categoryid)
        for offset in range(0, int(itemcount), 100):
            headers = {
                'Authorization' : config.ROOT_KEY,
                'Environment'    : Environment,
                'limit'    : "100",
                'offset'    : str(offset)
            }
            headers.update(extraheaders)
            response, rawBody = http.request(uri=url, method="GET", headers=headers)
            if response['status'] == "200":
                data = json.loads(rawBody)
                resitems = data.get('data', [])
                if len(data.get('data', [])) == 0:
                    break
                for item in resitems:
                    singleitem = {}
                    singleitem[item['itemId']] = item['title']
                    items.append(singleitem)
        return items
    except Exception, e:
        print str(e)

def showitem_title_with_itemspecifics(sellerid, locale, categoryid, Environment):
    leafcategories = get_leaf_categories(sellerid, categoryid)
    final = []
    for categoryid in leafcategories:
        itemspecifics = get_category_recommended_item_specifics(categoryid, locale)
        print "Current Category is " + str(categoryid) + " its item specifices is " + str(itemspecifics)
        items = seller_items(sellerid, categoryid, Environment, extraheaders={})
        for item in items:
            temp = {}
            res = scan_title_for_single_item(item[item.keys()[0]], itemspecifics, specifickey)
            temp['title'] = item[item.keys()[0]]
            temp['found'] = res
            print temp
            final.append(temp)
    return final
             

if __name__ == '__main__':
    Environment = "DEVELOPMENT"
    initialize(Environment)
    algo = "levenstien_distance"
    specifickey = None
    ngram = 2
    maxngram = 2
    
    
    locale = sys.argv[1]
    sellerid = sys.argv[2]
    categoryid = None
    if len(sys.argv) >= 4:
        categoryid = sys.argv[3]
    final = showitem_title_with_itemspecifics(sellerid, locale, categoryid, Environment)
    

        
