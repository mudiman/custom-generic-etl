'''
Created on Mar 14, 2012

@author: mudassar
'''

from etl.config import logger, config
from httplib2 import Http
import json
from etl.dal import insert_suggestion
from etl.system.system import initialize
from etl.miscfunctions import encodeobj



def get_item_count(sellerid, Environment, extraheaders={}):
    try:
        count = 0
        http = Http()
        url = config.SERVICE_BASE + "/sellers/%s/items" % sellerid
        headers = {
            'Authorization' : config.ROOT_KEY,
            'Environment'    : Environment
            }
        headers.update(extraheaders)
        response, rawBody = http.request(uri=url, method="GET", headers=headers)
        if response['status'] != "200":
            #logger.critical("error in items call for solr indexing for sellerid "+str(sellerid))
            return count
        
        count = response.get('totalcount', 0)
        return count
    except Exception, e:
        raise e
    
        
def get_csv(sellerid, offset, Environment, extraheaders=None):
    try:
        http = Http()
        seriviceurl = config.SERVICE_BASE + "/sellers/%s/items.csv" % sellerid
        headers = {
            'Authorization' : config.ROOT_KEY,
            'Environment'    : Environment,
            'limit'    : "100",
            'offset'    : offset
        }
        headers.update(extraheaders)
        response, rawBody = http.request(uri=seriviceurl, method="GET", headers=headers)
        return rawBody
    except Exception, e:
        logger.critical(seriviceurl + "  offset " + str(offset))
        raise e


def aggregate_field_value(value):
    try:
        res = dict(
          missing=0,
          invalid=0
        )
        for i in range(0, len(value), 2):
            valuename = value[i]
            count = value[i + 1]
            if valuename.find('(missing)') >= 0:
                res['missing'] += count
            elif valuename.find('COMPANY-invalid-$') >= 0:
                res['invalid'] += count
            
        
    except Exception, e:
        logger.exception(str(e))
    finally:
        return res
    

def replace_special_char_from_field(fieldslist):
    newfieldslist = []
    replace_characters = ["%5c", "%5c+", "%5c-", "%5c!", "%5c(", "%5c)", "%5c{", "%5c}", "%5c[", "%5c]", "%5c^", '%5c"', "%5c~", "%5c*", "%5c?", "%5c:"];
    special_characters = [" ", "+", "-", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":"];
    
    for field in fieldslist:
        temp = encodeobj(field)
# commented it was causing problem
#        for i,rpc in enumerate(replace_characters):
#            temp=temp.replace(special_characters[i],rpc)
        newfieldslist.append(temp)
    
    return newfieldslist


def replace_special_char_for_solr(fieldslist):
    newfieldslist = []
    replace_characters = ["\ ", "\+", "\-", "\!", "\(", "\)", "\{", "\}", "\[", "\]", "\^", '\"', "\~", "\*", "\?", "\:"]
    special_characters = [" ", "+", "-", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":"]
    
    for field in fieldslist:
        temp = encodeobj(field)
        
        for i, rpc in enumerate(replace_characters):
            temp = temp.replace(special_characters[i], rpc)
        newfieldslist.append(temp)
    
    return newfieldslist

def get_facet_field(sellerid, categoryid=None, fieldslist=[]):
    try:
        http = Http()
        newfieldslist = replace_special_char_from_field(fieldslist)
        fieldliststring = str(newfieldslist).replace("\'", "\"")
        newheader = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'FACETED_FIELD':fieldliststring}
        if categoryid != None:
            url = config.SERVICE_BASE + "/sellers/%s/categories/%s/items" % (sellerid, categoryid)
        else:
            url = config.SERVICE_BASE + "/sellers/%s/items" % (sellerid)
        url = str(url)
        response, rawBody = http.request(uri=url, method="GET", headers=newheader)
        data = json.loads(rawBody)
        facet_fields = data.get('facet_fields', {})
        return facet_fields
    except Exception, e:
        logger.exception(str(e))
        
        
if __name__ == "__main__":
    
    initialize("DEVELOPMENT")
    print get_facet_field('buy_united_kingdom@GB', 174, ["Type"])
