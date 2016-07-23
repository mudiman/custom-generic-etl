'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''

from etl.exceptions import TransformerException
from etl.config import logger
from operator import itemgetter
import json
from etl.tranformers.ebay.checks import convert_and_check_xml



def create_dummy_basic_item():
    obj = {}
    obj['itemId'] = None
    obj['title'] = None
    obj['thumbUrl'] = None
    obj['galleryURL'] = None
    obj['galleryPlusPictureURL'] = None
    obj['currentPrice'] = None
    obj['categories_categoryid'] = None
    obj['primarycategoryname'] = None
    obj['secondarycategoryid'] = 0
    obj['convertedCurrentPrice'] = None
    obj['viewItemURL'] = None
    obj['storename'] = None
    obj['sellerUserName'] = None
    return obj
    
def create_dummy_category():
    category = {}
    category['categoryid'] = None
    category['categoryName'] = None
    category['count'] = None
    category['parentcategoryid'] = None
    category['aspect'] = None
    category['childcategories'] = []
    return category
   
@convert_and_check_xml("FINDINGAPI")   
def parse_categoryHistogramContainer(**kwargs):
        try: 
            xml = kwargs.get('xml', None)
            category = kwargs.get('categoryid', None)
            result = create_dummy_category()
            scannode = 'categoryHistogramContainer/categoryHistogram/childCategoryHistogram'
            if category == None:scannode = 'categoryHistogramContainer/categoryHistogram'    
            if xml.find('categoryHistogramContainer/categoryHistogram') != None:
                for childCategoryHistogram in xml.findall(scannode):
                    tempCategory = create_dummy_category()
                    tempCategory['categoryid'] = int(childCategoryHistogram.findtext('categoryId'))
                    tempCategory['categoryName'] = childCategoryHistogram.findtext('categoryName', '').encode('utf-8')
                    tempCategory['count'] = int(childCategoryHistogram.findtext('count'))
                    if category: tempCategory['parentcategoryid'] = category
                    if not result['childcategories']:
                        result['childcategories'] = []
                    result['childcategories'].append(tempCategory)
        except Exception, e:
            logger.exception(str(e))
            raise TransformerException(str(e))
        finally:
            return result

@convert_and_check_xml("FINDINGAPI")   
def parse_for_aspects(**kwargs):
        try:
            xml = kwargs.get('xml', None)
            aspectsarray = {}
            for aspect in xml.findall('aspectHistogramContainer/aspect'):
                aspectsarray[aspect.attrib['name'].encode('utf-8')] = {}           
                for valueHistogram in aspect.findall('valueHistogram'):
                    aspectsarray[aspect.attrib['name'].encode('utf-8')][valueHistogram.attrib['valueName'].encode('utf-8')] = int(valueHistogram.findtext('count'))  
        except Exception, e:
            logger.exception(str(e))
            raise TransformerException(str(e))
        finally:
            if len(aspectsarray) > 0:
                return json.dumps(aspectsarray)
            else:
                return None
     
@convert_and_check_xml("FINDINGAPI")  
def parse_for_items(**kwargs):
    """
        Parsing Finding api FindingApiAdvance call from items
    """    
    try:
        xml = kwargs.get('xml', None)
        itemlist = []
        for item in xml.findall('searchResult/item'):
            try:
                obj = create_dummy_basic_item()
                obj['itemId'] = long(item.findtext('itemId'))
                #obj['categories_categoryid'] = item.findtext('primaryCategory/categoryId')
                obj['title'] = item.findtext('title', '').encode('utf-8')
                obj['thumbUrl'] = item.findtext('galleryURL', '').encode('utf-8')
                obj['viewItemURL'] = item.findtext('viewItemURL', '').encode('utf-8')
                obj['galleryPlusPictureURL'] = item.findtext('galleryPlusPictureURL', '').encode('utf-8')
                obj['currentPrice'] = float(item.findtext('sellingStatus/currentPrice', 0.0))
                obj['primarycategoryname'] = item.findtext('primaryCategory/categoryName', '').encode('utf-8')
                obj['secondarycategoryid'] = item.findtext('secondaryCategory/categoryId', 0)
                obj['secondarycategoryname'] = item.findtext('secondaryCategory/categoryName', '').encode('utf-8')
                obj['convertedCurrentPrice'] = float(item.findtext('sellingStatus/convertedCurrentPrice', 0.0))
                obj['storename'] = item.findtext('storeInfo/storeName', '').encode('utf-8')
                obj['viewItemURL'] = item.findtext('viewItemURL', '').encode('utf-8')
                #obj['sellerUserName'] = item.findtext('sellerinfo/sellerUserName')                
            except Exception , e:
                logger.debug(str(e))
            finally:
                if map(itemgetter('itemId'), itemlist).count(obj['itemId']) == 0:itemlist.append(obj)
    except Exception , e:
            logger.exception(str(e))
            raise TransformerException(str(e))
    finally:
        return itemlist

if __name__ == "__main__":pass
    
