from httplib2 import Http
import time
from etl.config import logger, ETL_DEBUG_PRINT, PRINT_PROGRESS, config
import json
from subprocess import *
from etl.dal import update_sellers_status
from types import StringType, ListType, NoneType, DictType, LongType, IntType, \
    FloatType
from elementtree.ElementTree import fromstring
import cloud
from etl.config.drivers import THREADEDEBAY, PICLOUDEBAY
import os

def check_match_string_for_running_process(cmd,findstring):
    output = os.popen(cmd).read()
    output=output.split("\n")
    count=0
    for line in output:
        print line
        if line.find(findstring)>-1:
            count+=1
    if count>=2:
        return True
    return False


def checkbrand(word):
    if word in ["Brand",u'Marke',"brand"]:
        return "brand"
    else:
        return "brand"

def log_critical_message(msg,drivertype=None):
    if drivertype:
        driver=drivertype
    else:
        if config:
            try:
                driver=config.DRIVERTYPE
            except Exception,e:
                driver=PICLOUDEBAY
        else:
            driver=PICLOUDEBAY
    if driver==THREADEDEBAY:
        logger.critical(msg)
    elif driver==PICLOUDEBAY:
        cloud.cloud.cloudLog.critical(msg)


def log_exception_message(msg,drivertype=None):
    if drivertype:
        driver=drivertype
    else:
        if config:
            try:
                driver=config.DRIVERTYPE
            except Exception,e:
                driver=PICLOUDEBAY
        else:
            driver=PICLOUDEBAY
    if driver==THREADEDEBAY:
        logger.exception(msg)
    elif driver==PICLOUDEBAY:
        cloud.cloud.cloudLog.exception(msg)



def remove_namespace(doc, namespace):
    """
        Remove namespace in the passed document in place.
    """
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]
            
            
def create_list(entrylist):
    temps = []
    if entrylist.__class__.__name__ == "list":
        temps = entrylist
    else:
        temps.append(entrylist)
    return temps


def update_seller_status(phase, prog=True):
    """
        Prog specifics to updated progress at start and end of phase if true
    """
    def wrap(func):
        def decorator(self, *args, **kwargs):
            resp = func(self, *args, **kwargs)    
            return resp
        return decorator
    return wrap


def cache_raw_data(func):
    """
        Cache raw response from etl extractors
    """
    def decorator(**kwargs):
        return func(**kwargs)
    return decorator


def save_progress_locally(local=False, selleridkey=None, temp=None):
    if local == True:
        obj = dict(sellerid=selleridkey)
        update_sellers_status([obj], temp)
        
incrementalprogress = 0

def print_progress(phase=None, progress=None, local=False):
    """
        Prog specifics to updated progress at start and end of phase if true
    """
    
    def wrap(func):
        def decorator(self, *args, **kwargs):
            global incrementalprogress
            if progress == None:
                incrementalprogress = 0
                temp = dict(phase=phase, progress="STARTED")
                if PRINT_PROGRESS:
                    print json.dumps(temp)
                save_progress_locally(local, self.selleridkey, temp)
                resp = func(self, *args, **kwargs)
                temp = dict(phase=phase, progress="ENDED")
                if PRINT_PROGRESS:
                    print json.dumps(temp)
                save_progress_locally(local, self.selleridkey, temp)
            else:
                incrementalprogress += 1
                temp = dict(phase=phase, progress=str(incrementalprogress))
                if PRINT_PROGRESS:
                    print json.dumps(temp)
                save_progress_locally(local, self.selleridkey, temp)
                resp = func(self, *args, **kwargs)

            return resp
        return decorator
    return wrap

def parselastprogress(stddata):
    dummyprogress = {"phase": "NONE", "progress": "NONE"}
    try:
        output = stddata.split("\n")
        for i in range(len(output) - 1, 0, -1):
            try:
                dummyprogress = json.loads(output[i])
                break
            except Exception, e:
                pass
    except Exception, e:
        pass
    finally:
        return dummyprogress

def printprogress(phase, process):
    temp = dict(phase=phase, progress=process)
    if PRINT_PROGRESS:
        print json.dumps(temp)


def encodeobj(obj):
    if type(obj) == FloatType or type(obj) == IntType or type(obj) == LongType:
        return str(obj)
    elif type(obj) == NoneType:
        return ""
    elif type(obj) == ListType or type(obj) == DictType:
        return str(obj)
    elif type(obj) == StringType:
        return obj
    else:
        return obj.encode('utf-8')


def getvalue_from_list_with_key(datalist, key):
    for item in datalist:
        if item.get(key):
            return item.get(key)
    return 0

def httpcallswrapper(url=None, method=None, body=None, headers=None):
    fibonic = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    http = Http()
    for i in range(0, len(fibonic)):
        try:
            responseheaders, rawbody = http.request(uri=url, method=method, body=body, headers=headers)
            if str(responseheaders['status']) != '200':
                if ETL_DEBUG_PRINT:
                    print "failed " + str(url)
                    #log_critical_message("failed " + str(url))
                time.sleep(fibonic[i])
            else:
                isxml = False
                try:
                    xml = fromstring(rawbody) 
                    namespaces = ['http://www.ebay.com/marketplace/search/v1/services', 'urn:ebay:apis:eBLBaseComponents']
                    for namespace in namespaces:
                        remove_namespace(xml, namespace)
                    if xml.findtext('ack') in ["Failure", "Error"] or xml.findtext('Ack') in ["Failure", "Error"]:
                        print "ACK failure retrying status code " +str(responseheaders)+" on url "+ str(url)
                        #log_critical_message("ACK failure retrying status code " +str(responseheaders)+" on url "+ str(url))
                        print str(rawbody)
                        #log_critical_message(str(rawbody))
                        time.sleep(fibonic[i])
                    else:
                        return [responseheaders, rawbody]
                except Exception, e:
                    print e
                if isxml == False:
                    return [responseheaders, rawbody]
                
        except Exception, e:
            log_exception_message(str(e))
            time.sleep(fibonic[i])
    return [None, None]
        
if __name__ == "__main__":
    ss = """{"PHASE": "TEST", "PROGRESS": "TEST"}\nsadasdsa\nfdf89fy9sd8h90\n{"PHASE": "TEST", "PROGRESS": "TEST"}"""
    parselastprogress(ss)
        
