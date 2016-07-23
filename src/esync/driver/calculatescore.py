'''
Created on May 2, 2012

@author: asimriaz
'''
from httplib2 import Http
from etl.config import config

from esync.dumps import readfiltersummary, writedump



def calculate_score_for(sellersItemList):
        
        for sellerid in sellersItemList:

            base_url = config.SCORING_SERVICE_BASE + "/sellers/%s" % sellerid

            cat_count = 0
            valid_category_count = 0
            for cat in sellersItemList[sellerid]:
                if  len(sellersItemList[sellerid][cat].get("ItemRevised", [])) > 0 \
                    or len(sellersItemList[sellerid][cat].get("ItemClosed", [])) > 0 or len(sellersItemList[sellerid][cat].get("ItemListed", [])) > 0  :
                    valid_category_count = valid_category_count + 1
            for categoryid in sellersItemList[sellerid]:
                category_detail = sellersItemList[sellerid][categoryid]
                if  len(category_detail.get("ItemRevised", [])) > 0 \
                    or len(category_detail.get("ItemClosed", [])) > 0 or len(category_detail.get("ItemListed", [])) > 0  :
                    cat_count = cat_count + 1
                    category_url = base_url + '/categories/%s/datascore' % str(categoryid)
                    newheader = {'AUTHORIZATION':config.ROOT_KEY, 'ENVIRONMENT':config.ENVIRONMENT, 'locale':sellerid.split('@')[1]}
                    if cat_count != valid_category_count:
#                        newheader["AGGREGATESCORE"] ="False"
                        print "only calculating score"      
                    else:
                        print "calculating scoring with aggregate score for categories"
                    
                    postUrl(category_url, newheader)
                    


        
def postUrl(url, headers):
    """
        This function makes an http call and rturns the result
    """

    try:
        response = None
        for i in range(1, 4):               
            try:
                http = Http()
                print url
                response, rawBody = http.request(uri=url, method="POST", headers=headers)
                break
            except Exception, e:
                
                print "Attempt %s failed because of %s" % (i, str(e)) 
                if i == 3:
                    raise        
        return response
    except Exception, e:
        raise e
    
if __name__ == '__main__':
    from esync.driver.main import initialize
    initialize("DEVELOPMENT")
    date = "2012-5-3-17"
    sellersItemList = readfiltersummary(date, type="notifications")
    calculate_score_for(sellersItemList)
