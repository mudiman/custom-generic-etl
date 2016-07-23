'''
Created on Oct 7, 2011

@author: Mudassar Ali
'''
from etl.config.transformer.ebay import getsanitylogger







def aspectSanityCheck(f):    
    def decorator(**kwargs):
        sanitylogger = getsanitylogger()
        d = {}
        d['sellerid'] = kwargs.get('sellerid', None)
        d['itemid'] = None
        d['categoryid'] = kwargs.get('categoryid', None)
        
        xml = kwargs.get('xml', None)
        try:
            aspectsxml = xml.findall('aspectHistogramContainer/aspect')
            if len(aspectsxml) == 0:
                sanitylogger.debug("Sanity Check Failure as no aspect for category ", extra=d)
            for aspect in aspectsxml:
                if not aspect.attrib['name'].encode('utf-8'):
                    sanitylogger.debug("Sanity Check Failure as aspect has no attribute aspect name", extra=d)
                valueHistograms = aspect.findall('valueHistogram')
                if not valueHistograms:
                    sanitylogger.debug("Sanity Check Failure as aspect does not have values", extra=d)
                for valueHistogram in valueHistograms:
                    if not valueHistogram.attrib['valueName'].encode('utf-8'):
                        sanitylogger.debug("Sanity Check Failure as aspect values does not have valuename", extra=d)
                    if not valueHistogram.find('count').text:
                        sanitylogger.debug("Sanity Check Failure as aspect values does not have count", extra=d)
        except Exception, e:
            sanitylogger.debug("Sanity Check Failure as no aspect for category ", extra=d)
        
                
        return f(**kwargs)
    return decorator

def categoryhistogramSanityCheck(f):    
    def decorator(**kwargs):
        sanitylogger = getsanitylogger()
        xml = kwargs.get('xml', None)
        d = {}
        d['sellerid'] = kwargs.get('sellerid', None)
        d['itemid'] = None
        d['categoryid'] = kwargs.get('categoryid', None)
        
        if xml.find('categoryHistogramContainer/categoryHistogram') != None:
            for childCategoryHistogram in xml.findall('categoryHistogramContainer/categoryHistogram/childCategoryHistogram'):            
                if not childCategoryHistogram.find('categoryId').text:
                    sanitylogger.debug("Sanity Check Failure as no categoryid in categoryhistogram for category ", extra=d)
                if not childCategoryHistogram.find('categoryName').text.encode('utf-8'):
                    sanitylogger.debug("Sanity Check Failure as no categoryName in categoryhistogram for category ", extra=d)
                if not childCategoryHistogram.find('count').text:
                    sanitylogger.debug("Sanity Check Failure as no count categoryhistogram for category ", extra=d)                        
        else:
            sanitylogger.debug("Sanity Check Failure as no categoryhistogram for category ", extra=d)
        return f(**kwargs)
    return decorator

                                    
def itemsSanityCheck(f):    
    def decorator(**kwargs):
        sanitylogger = getsanitylogger()
        xml = kwargs.get('xml', None)
        for item in xml.findall('searchResult/item'):
            try:
                d = {}
                d['sellerid'] = kwargs.get('sellerid', None)
                d['itemid'] = None
                d['categoryid'] = kwargs.get('categoryid', None)
                if not item.find('itemId').text:
                    d['itemid'] = item.find('itemId').text
                    sanitylogger.debug("Sanity Check Failure for item property itemId", extra=d)
                if item.find('title') != None:
                    sanitylogger.debug("Sanity Check Failure for item property title", extra=d)
                if item.find('galleryURL') != None:
                    sanitylogger.debug("Sanity Check Failure for item property galleryURL", extra=d)
                if item.find('galleryPlusPictureURL') != None:
                    sanitylogger.debug("Sanity Check Failure for item property galleryPlusPictureURL", extra=d)  
                if item.find('sellingStatus/currentPrice') != None:
                    sanitylogger.debug("Sanity Check Failure for item property sellingStatus/currentPrice", extra=d)
                if item.find('primaryCategory/categoryId') != None:
                    sanitylogger.debug("Sanity Check Failure for item property primaryCategory/categoryId", extra=d)
                if item.find('primaryCategory/categoryName') != None:
                    sanitylogger.debug("Sanity Check Failure for item property primaryCategory/categoryName", extra=d)
                if item.find('secondaryCategory/categoryId') != None:
                    sanitylogger.debug("Sanity Check Failure for item property secondaryCategory/categoryId", extra=d)
                if item.find('secondaryCategory/categoryName') != None:
                    sanitylogger.debug("Sanity Check Failure for item property secondaryCategory/categoryName", extra=d)
                if item.find('sellingStatus/convertedCurrentPrice') != None:
                    sanitylogger.debug("Sanity Check Failure for item property sellingStatus/convertedCurrentPrice", extra=d)
                if item.find('storeInfo/storeName') != None:
                    sanitylogger.debug("Sanity Check Failure for item property storeInfo/storeName", extra=d)
                if item.find('viewItemURL') != None:
                    sanitylogger.debug("Sanity Check Failure for item property viewItemURL", extra=d)
            except Exception , e:
                sanitylogger.debug(str(e), extra=d)

        return f(**kwargs)
    return decorator
