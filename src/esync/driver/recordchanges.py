'''
Created on Aug 10, 2012

@author: mudassar
'''
from httplib2 import Http
import json
import time
from datetime import date as datee, timedelta
import datetime



try:
    import esync
except ImportError:
    import os
    import sys
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    print p
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)


from esync.driver.loaddata import save_change
from esync.dumps import readfiltersummary
from etl.config import config
from etl.dal.scoringdal import sss_for_history

def record_changes(date=None):
    sellers = readfiltersummary(date, "notifications")
    for seller, sellerdata in sellers.items():
        for category, change in sellerdata.items():
            itemids = change.get('ItemRevised', [])
            for item in itemids:
                currenttime = datetime.datetime.strptime(date, "%Y-%m-%d-%f")
                #currenttime=time.strftime("%Y-%m-%d",time.localtime())
                yesterday = currenttime - timedelta(60)
                yesterday = yesterday.strftime("%Y-%m-%d")
                temp = sss_for_history(item, seller, yesterday, datetime.datetime.strftime(currenttime, "%Y-%m-%d"))
                if temp.get('error'):
                    print temp.get('error')
                    continue
                itemdata = temp['data']
                keys = itemdata.keys()
                keys.sort()
                if len(keys) > 1:
                    newdatascore = itemdata.get(keys[len(keys) - 1], {}).get('dataScore', {}).get('totalScore', None)
                    olddatascore = itemdata.get(keys[len(keys) - 2], {}).get('dataScore', {}).get('totalScore', None)
                    if olddatascore != newdatascore:
                        save_change(seller, category, item, "DATASCORECHANGE", olddatascore, newdatascore)
                    
                    newprice = itemdata.get(keys[len(keys) - 1], {}).get('price', 0)
                    olddprice = itemdata.get(keys[len(keys) - 2], {}).get('price', 0)
                    if olddprice != newprice:
                        save_change(seller, category, item, "PRICECHANGE", olddprice, newprice)
                    
                    
                    
            
    
    
if __name__ == '__main__':
    from etl.system.system import initialize
    initialize("DEVELOPMENT")
    import sys
    record_changes(sys.argv[1])
