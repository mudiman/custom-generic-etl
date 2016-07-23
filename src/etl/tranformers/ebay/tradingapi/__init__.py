'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''

from etl.exceptions import TransformerException
from etl.config import logger
from etl.tranformers.ebay.checks import convert_and_check_xml

def create_dummy_detail_item():
    obj = {}
    obj['itemId'] = None
    obj['itemspecifics'] = []
    obj['variation'] = []
    obj['galleryurl'] = None
    obj['photodisplay'] = None
    return obj

@convert_and_check_xml("TRADINGAPI")
def parse_trading_api_get_item(**kwargs):    
    """
        Parse GetItem tradingapi call xml
    """
    try:
        xml = kwargs.get('xml', None)
        obj = create_dummy_detail_item()
        
        obj['itemId'] = long(xml.findtext('Item/ItemID'))
        obj['galleryurl'] = str(xml.findtext('Item/PictureDetails/GalleryURL', '').encode('utf-8'))
        obj['photodisplay'] = str(xml.findtext('Item/PictureDetails/PhotoDisplay', '').encode('utf-8'))
        obj['quantity'] = int(xml.findtext('Item/Quantity', 0))
        obj['quantitySold'] = int(xml.findtext('Item/SellingStatus/QuantitySold', 0))
        obj['currentPrice'] = float(xml.findtext('Item/SellingStatus/CurrentPrice', 0.0))
        obj['buy_now_price'] = float(xml.findtext('Item/BuyItNowPrice', 0))
        
        
        obj['itemsku'] = xml.findtext('Item/SKU', '').encode('utf-8')
        
        obj['title'] = xml.findtext('Item/Title', '').encode('utf-8')
        obj['thumbUrl'] = xml.findtext('Item/PictureDetails/GalleryURL', '').encode('utf-8')
        obj['galleryPlusPictureURL'] = xml.findtext('Item/PictureDetails/PictureURL', '').encode('utf-8')
        
        obj['primarycategoryname'] = xml.findtext('Item/PrimaryCategory/CategoryName', '').encode('utf-8')
        obj['categories_categoryid'] = xml.findtext('Item/PrimaryCategory/CategoryID', 0)
        obj['secondarycategoryid'] = xml.findtext('Item/SecondaryCategory/CategoryID', 0)
        obj['secondarycategoryname'] = xml.findtext('Item/SecondaryCategory/CategoryName', '').encode('utf-8')
        obj['convertedCurrentPrice'] = float(xml.findtext('Item/SellingStatus/ConvertedCurrentPrice', 0.0))

        #obj['viewItemURL'] = xml.findtext('Item/ListingDetails/ViewItemURL', '').encode('utf-8')
        
        obj['storename'] = xml.findtext('Item/Seller/UserID', '').encode('utf-8')
        obj['sellerid'] = xml.findtext('Item/Seller/UserID', '').encode('utf-8') + "@" + xml.findtext('Item/Country', '').encode('utf-8')
        
        
        itsp = {}
        for tt in xml.findall('Item/ItemSpecifics/NameValueList'):
            if tt.findtext('Name'):
                itsp[tt.findtext('Name', '').encode('utf-8').strip()] = tt.findtext('Value', '').encode('utf-8').strip()
        if not itsp.get('Condition'): 
            itsp['Condition'] = xml.findtext('Item/ConditionDisplayName', '').encode('utf-8')
                
        if len(itsp) > 0:obj['itemspecifics'] = itsp
        itemvariations = []                         
        for variation in xml.findall('Item/Variations/Variation'):
            try:
                varObj = {}
                varObj['SKU'] = variation.findtext('SKU', '').encode('utf-8')
                varObj['Quantity'] = int(variation.findtext('Quantity', 0))
                varObj['QuantitySold'] = int(variation.findtext('SellingStatus/QuantitySold', 0))
                for subSpe in variation.findall('VariationSpecifics/NameValueList'):
                    varObj[subSpe.findtext('Name', '').encode('utf-8').strip()] = subSpe.findtext('Value', '').encode('utf-8').strip()   
            except Exception, e:
                logger.debug(str(e))
            finally:
                itemvariations += [varObj]
        if len(itemvariations) > 0:
            obj['variation'] = itemvariations
        else:
            obj['variation'] = []
        
    except Exception , e:
        logger.exception(str(e))
        raise TransformerException(str(e))
    finally:
        return obj


@convert_and_check_xml("TRADINGAPI")  
def parse_check_token(**kwargs):
    try:
        xml = kwargs.get('xml', None)
        if xml.findtext('TokenStatus/Status', 'Failure') != 'Active':
            return False
    except Exception , e:
        logger.exception(str(e))
        raise TransformerException(str(e))
    finally:
        return True
    
@convert_and_check_xml("TRADINGAPI")  
def parse_get_token_user(**kwargs):
    try:
        xml = kwargs.get('xml', None)
        
    except Exception , e:
        logger.exception(str(e))
        raise TransformerException(str(e))
    finally:
        return xml.findtext('User/UserID', False)

if __name__ == "__main__":
    with open("196", 'r') as f:
        temp = f.read()
    parse_trading_api_get_item(xml=temp)
    pass
