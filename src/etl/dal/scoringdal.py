'''
Created on Mar 14, 2012

@author: mudassar
'''
from etl.config import logger, config
from httplib2 import Http
from etl.system.system import initialize
import json



def calculate_score(sellerid):
    try:
        http = Http()
        newheader = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT,'RECORDMISSING':'True','locale':sellerid.split('@')[1]}
        url = config.SCORING_SERVICE_BASE + "/sellers/%s/datascore" % sellerid
        response, rawBody = http.request(uri=url, method="POST", headers=newheader)
        logger.info(response)
    except Exception, e:
        logger.exception(str(e))
        
def sss_for_category_history(sellerid, categoryid, fromd, uptod):
    url = config.SCORING_SERVICE_BASE + "/sellers/%s/categories/%s/datascore/%s/upto/%s" % (sellerid, categoryid, fromd, uptod)
    headers = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'locale':sellerid.split('@')[1]}
    try:
        response = None
        for i in range(1, 4):               
            try:
                http = Http()
                print url
                response, rawBody = http.request(uri=url, method="GET", headers=headers)
                break
            except Exception, e:
                
                print "Attempt %s failed because of %s" % (i, str(e)) 
                if i == 3:
                    raise        
        return json.loads(rawBody)
    except Exception, e:
        raise e


def sss_for_history(item, sellerid, fromd, uptod):
    url = config.SCORING_SERVICE_BASE + "/items/%s/datascore/%s/upto/%s" % (item, fromd, uptod)
    headers = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'locale':sellerid.split('@')[1]}
    try:
        response = None
        for i in range(1, 4):               
            try:
                http = Http()
                print url
                response, rawBody = http.request(uri=url, method="GET", headers=headers)
                break
            except Exception, e:
                
                print "Attempt %s failed because of %s" % (i, str(e)) 
                if i == 3:
                    raise        
        return json.loads(rawBody)
    except Exception, e:
        raise e
    
if __name__ == "__main__":
    
    initialize("DEVELOPMENT")
    print calculate_score("officeshoes@GB")
