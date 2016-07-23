'''
Created on Apr 25, 2012

@author: asimriaz
'''
import Queue
import urllib2
import json
from BeautifulSoup import BeautifulSoup
from threading import Lock


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
    
from etl.lib.ThreadPool.ThreadPool import ThreadPool
from esync.dumps import readsummary, writedump, copyfile
from etl.config import logger






def check_item_authentication(sellerid=None, categoryid=None, itemid=None, revision_count=0, base_url="", allowed_values=None, queue=None,date=None):
    """
        This is the method that checks if the item that is provided has a valid notification according
        to company standards and appends to a temporary list if it is valid
    
    """
    
    
    response = {
                    "sellerid":sellerid,
                    "categoryid":categoryid,
                    "itemid":None
                    }
    if check_item_html(base_url, itemid, revision_count, allowed_values,date):
        response["itemid"] = itemid
    queue.put(response)
    

def saveFile(queue, sellersItemList, date):
    #filtering the items on the basis of notifications 
    sellersItemTempList = {}
    for i in range(queue.qsize()):
            result = queue.get()
            sellerid = result["sellerid"]
            categoryid = result["categoryid"]
            itemid = result["itemid"]
            if sellerid not in sellersItemTempList:
                sellersItemTempList[sellerid] = {}
            if categoryid not in sellersItemTempList[sellerid]:
                sellersItemTempList[sellerid][categoryid] = []
            if itemid != None:
                sellersItemTempList[sellerid][categoryid].append(itemid)
            queue.task_done()    
    queue.join()      
    for seller in sellersItemTempList:
        for category in sellersItemTempList[seller]:     
            sellersItemList[seller][category]["ItemRevised"] = []
            for itemid in sellersItemTempList[seller][category]:
                sellersItemList[seller][category]["ItemRevised"].append(itemid)
    writedump(json.dumps(sellersItemList), date, "notifications", "filtersummary.json")



def check_item_html(base_url, itemid, revision_count, allowed_values,date):
    """
        This function makes the http calls and checks the validation of notification and retunrs
        true or false
        
    
    """
    data = getPage(base_url + str(itemid))    
    soup = BeautifulSoup(data) 
    try:
        total_tbl = len(soup.findAll('div', attrs={"class":'pagecontainer'})[0].findAll("table"))
        tbl_number = 2
        # this is a fix because some times ebay returns more tables
        if total_tbl == 8:
            tbl_number = 3
        try:
            trs = soup.findAll('div', attrs={"class":'pagecontainer'})[0].findAll("table")[tbl_number].findAll("table")[0].findAll("tr")[-revision_count:]
        except Exception,e:
            trs=[]
        for tr in trs:
            try:
                td_data = tr.findAll("td", attrs={"width":"30%"})[0].text
                for value in allowed_values:
                    if td_data.find(value) > -1:
                        return True
            except Exception,e:
                pass
            #logger.exception(e)
        return False
    except Exception, e:
        print base_url + str(itemid)
        outfile = file('output.txt', 'w') # Pay attention, with this instruction you will overwrite
        outfile.write(data)  # an existing file with the same name without any warning!
        outfile.close()
        logger.exception(e)
        print e, "for itemid %s" % str(itemid)
        return False


def getPage(url):
    """
        This function makes an http call and rturns the result
    """

    try:
        data = None
        
        for i in range(1, 4):               
            try:
                user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/11.10 Chromium/18.0.1025.168 Chrome/18.0.1025.168 Safari/535.19"
                headers = {"User-Agent":user_agent}
                
                req = urllib2.Request(url, "", headers)
                f = urllib2.urlopen(req)
                data = f.read()
                f.close()
                
                break
            except Exception, e:
                data = None
                print "Attempt %s failed because of %s" % (i, str(e)) 
                if i == 3:
                    raise        
        return data
    except Exception, e:
        raise e




def save_authentic_revised_items(date, sellersItemList):
    """
        This function takes date as an argument.
        reads the summary.json file from that date folder then gets upadted item list and checks the validation of the notification of the item
        and appends it to the temp list and writes a filtersummary.json file
    
    """

    queue = Queue.Queue()
    base_url = "http://cgi.ebay.co.uk/ws/eBayISAPI.dll?ViewItemRevisionDetails&_trksid=p4340.l2569&rt=nc&item="
    allowed_values = ["Item Specifics", "Quantity", "Buy it now price"]
    threadpool = ThreadPool(20)
    #adding threads to verify if the item notification received is of quantity or itemspecific
    for seller in sellersItemList:
        for category in sellersItemList[seller]:
            if "ItemRevised" in sellersItemList[seller][category]:
                for itemid, revision_count in sellersItemList[seller][category]["ItemRevised"].items():
                    threadpool.add_task(check_item_authentication, sellerid=seller, categoryid=category, itemid=itemid, revision_count=revision_count, base_url=base_url, allowed_values=allowed_values, queue=queue,date=date)
    
    threadpool.wait_completion()
    saveFile(queue, sellersItemList, date)
                
if __name__ == "__main__":
    import sys
    date = sys.argv[1]
    
#    base_url = "http://cgi.ebay.co.uk/ws/eBayISAPI.dll?ViewItemRevisionDetails&_trksid=p4340.l2569&rt=nc&item="
#    allowed_values = ["Item Specifics", "Quantity", "Buy it now price"]
#    check_item_authentication(sellerid=None, categoryid=None, itemid="170900465545", revision_count=0, base_url=base_url, allowed_values=allowed_values, queue=None,date=None)
    sellersItemList = readsummary(date, type="notifications")
    save_authentic_revised_items(date, sellersItemList)


