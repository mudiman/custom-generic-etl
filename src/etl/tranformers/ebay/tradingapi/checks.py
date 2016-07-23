'''
Created on Oct 7, 2011

@author: Mudassar Ali
'''
from etl.config.transformer.ebay import getsanitylogger



def itemspecificsAndVaritionChecks(f):    
    def decorator(self, *args, **kwargs):
        sanitylogger = getsanitylogger()
        d = sellerdict()
        d['itemid'] = self.itemid
        d['categoryid'] = ""
        try:
            if self.responsexml.find('Item/PictureDetails/GalleryURL') == None:
                sanitylogger.debug("Sanity Check Failure for item property galleryurl", extra=d)
            if self.responsexml.find('Item/PictureDetails/PhotoDisplay') == None:
                sanitylogger.debug("Sanity Check Failure for item property photodisplay", extra=d)
            if self.responsexml.findall('Item/ItemSpecifics/NameValueList') == None:
                sanitylogger.debug("Sanity Check Failure for item property itemspecifics", extra=d)
            else:
                for itsp in self.responsexml.findall('Item/ItemSpecifics/NameValueList'):
                    if itsp.find('Name').text.encode('utf-8') == None:
                        sanitylogger.debug("Sanity Check Failure for item's particular itemspecifics name", extra=d)
                    if itsp.find('Value').text == None:
                        sanitylogger.debug("Sanity Check Failure for item's particular itemspecifics value", extra=d)
            if self.responsexml.findall('Item/Variations/Variation') == None:
                sanitylogger.debug("Sanity Check Failure for item property variation", extra=d)
            else:
                for variation in self.responsexml.findall('Item/Variations/Variation'):
                    if variation.find('SKU') == None:
                        sanitylogger.debug("Sanity Check Failure for item variation SKU", extra=d)
                    if variation.find('Quantity') == None:
                        sanitylogger.debug("Sanity Check Failure for item variation Quantity", extra=d)
                    if variation.find('SellingStatus/QuantitySold') == None:
                        sanitylogger.debug("Sanity Check Failure for item variation QuantitySold", extra=d)
                    if variation.findall('VariationSpecifics/NameValueList') == None:
                        sanitylogger.debug("Sanity Check Failure for item variation VariationSpecifics", extra=d)
                    else:
                        for subSpe in variation.findall('VariationSpecifics/NameValueList'):
                            if subSpe.find('Name') == None:
                                sanitylogger.debug("Sanity Check Failure for item variation VariationSpecifics name", extra=d) 
                            if subSpe.find('Value') == None:
                                sanitylogger.debug("Sanity Check Failure for item variation VariationSpecifics value", extra=d)
            
        except Exception, e:
            sanitylogger.debug("Sanity Check Failure for item %s" % str(e), extra=d)
        finally:
            return f(self, *args, **kwargs)
    return decorator


                    
def bestmatchsanitycheck(f):    
    def decorator(self, *args, **kwargs):
        sanitylogger = getsanitylogger()
        d = sellerdict()
        d['itemid'] = ""
        d['categoryid'] = ""
        try:
            for singleItem in self.response.findall('item'):
                try:
                    if singleItem.find('itemId') == None:
                        sanitylogger.debug("Sanity Check Failure for item property itemId", extra=d)
                    else:
                        d['itemid'] = singleItem.find('itemId').text
                    if singleItem.find('primaryCategory/categoryId') == None:
                        sanitylogger.debug("Sanity Check Failure for item property categoryid", extra=d)
                    if singleItem.find('bestMatchData/salesCount') == None:
                        sanitylogger.debug("Sanity Check Failure for item property salesCount", extra=d)
                    if singleItem.find('quantityAvailable') == None:
                        sanitylogger.debug("Sanity Check Failure for item property quantityAvailable", extra=d)
                    if singleItem.find('quantitySold') == None:
                        sanitylogger.debug("Sanity Check Failure for item property quantitySold", extra=d)    
                    if singleItem.find('bestMatchData/viewItemCount') == None:
                        sanitylogger.debug("Sanity Check Failure for item property viewItemCount", extra=d)
                    if singleItem.find('bestMatchData/salesPerImpression') == None:
                        sanitylogger.debug("Sanity Check Failure for item property salesPerImpression", extra=d)
                    if singleItem.find('sellingStatus/currentPrice') == None:
                        sanitylogger.debug("Sanity Check Failure for item property currentPrice", extra=d)
                    if singleItem.find('bestMatchData/viewItemPerImpression') == None:
                        sanitylogger.debug("Sanity Check Failure for item property viewItemPerImpression", extra=d)
                    if singleItem.find('bestMatchData/impressionCountRange') == None:
                        sanitylogger.debug("Sanity Check Failure for item property impressionCount", extra=d)
                    if singleItem.find('bestMatchData/impressionCountRange/min') == None:
                        sanitylogger.debug("Sanity Check Failure for item property impressionCount", extra=d)
                    if singleItem.find('bestMatchData/impressionCountRange/max') == None:
                        sanitylogger.debug("Sanity Check Failure for item property impressionCount", extra=d)
                    impressioncount = (float(singleItem.find('bestMatchData/impressionCountRange/max').text) + float(singleItem.find('bestMatchData/impressionCountRange/min').text)) / 2
                    if float(impressioncount) < float(singleItem.find('bestMatchData/viewItemCount').text):
                        sanitylogger.debug("Sanity Check Failure for item ", extra=d)                    
                except Exception , e:
                    sanitylogger.debug("Sanity Check Failure for complete item %s" % str(e), extra=d)                    
        finally:
            return f(self, *args, **kwargs)
