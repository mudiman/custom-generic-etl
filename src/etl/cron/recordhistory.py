'''
Created on Aug 28, 2012

@author: mudassar
'''
from etl.system.system import initialize
from etl.config import config
from etl.dal import get_all_leafcategories_categoryidonly
import datetime
from datetime import timedelta
from etl.dal.scoringdal import sss_for_category_history

def record_data(sellerid=None):
    categories = get_all_leafcategories_categoryidonly(sellerid)
    res = []
    currenttime = datetime.datetime.now()
    yesterday = currenttime - timedelta(1)
    yesterday = yesterday.strftime("%Y-%m-%d")
    currenttime = datetime.datetime.strptime(currenttime, "%Y-%m-%d-%f")
    
    for category in categories:
        temp = {}
        
        data = sss_for_category_history(sellerid, category['categoryid'], yesterday, currenttime)
        data = data['data']
        
        temp['sellerid'] = sellerid;
        temp['categoryid'] = category['categoryid'];
        temp['name'] = category['name'];
        
        temp['expected_sales'] = category['expected_sales'];
        temp['actual_sales'] = category['actual_sales'];
        
        temp['expected_impression'] = category['expected_impression'];
        temp['actual_impression'] = category['actual_impression'];
        
        temp['expected_conversion'] = category['expected_conversion'];
        temp['actual_conversion'] = category['actual_conversion'];
        
        temp['quantity'] = category['quantity'];
        temp['parentcategoryid'] = category['parentcategoryid'];
        temp['roi'] = category['roi'];
        
        res.append(temp)
        
    
    return res
    
if __name__ == '__main__':
    initialize("DEVELOPMENT")
    
