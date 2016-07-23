'''
Created on Mar 14, 2012

@author: mudassar
'''

from etl.config import logger, config
from httplib2 import Http
import json
from etl.system.system import initialize



def categories_properties(categoryid, locale):
    try:
        http = Http()
        newheader = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'locale':locale}
        url = config.METADATA_SERVICE_BASE + "/categories/%s/properties" % categoryid
        response, rawBody = http.request(uri=url, method="GET", headers=newheader)
        data = json.loads(rawBody)
        return data.get('data', {}).keys()
        logger.info(response)
    except Exception, e:
        logger.exception(str(e))
        
        
if __name__ == "__main__":
    
    initialize("DEVELOPMENT")
    print categories_properties(11498, "GB")
