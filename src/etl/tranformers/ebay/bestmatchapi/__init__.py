'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''
from etl.exceptions import TransformerException
from etl.config import logger

import itertools
import operator
from etl.tranformers.ebay.checks import convert_and_check_xml
import collections
import functools

def create_dummy_item_bestmatch():
    result = {}
    result['itemId'] = None
    result['categoryid'] = None
    result['itemRank'] = 0
    result['salesCount'] = 0
    result['viewItemCount'] = 0
    result['salesPerImpression'] = 0.0
    result['currentPrice'] = 0.0
    result['conversionrate'] = 0.0
    result['quantityAvailable'] = 0.0
    result['quantitySold'] = 0.0
    result['viewItemPerImpression'] = 0.0
    result['impressionCountMin'] = 0
    result['impressionCountMax'] = 0
    result['impressionCount'] = 0

    return result
    
def aggregatesameitems(f):
    def decorator(**kwargs):
        items = f(**kwargs)
        itemlist = itertools.groupby(items, operator.itemgetter('itemId'))
        groups = []
        for k, g in itemlist:
            try:
                ls = list(g)
                temp = {}
                temp['itemId'] = ls[0]['itemId']
                temp['categoryid'] = ls[0]['categoryid']
#                temp['salesCount'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('salesCount'), ls))
                temp['quantityAvailable'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('quantityAvailable'), ls))
                temp['quantitySold'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('quantitySold'), ls))
                #temp['viewItemCount'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('viewItemCount'), ls))
                #temp['viewItemCount'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('viewItemCount'), ls))
                #temp['salesPerImpression'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('salesPerImpression'), ls))
                #temp['viewItemPerImpression'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('viewItemPerImpression'), ls))
                #temp['impressionCount'] = functools.reduce(lambda x, y: x + y, map(operator.itemgetter('impressionCount'), ls))

                temp['salesCount'] = ls[0]['salesCount']
                temp['viewItemCount'] = ls[0]['viewItemCount']
                temp['salesPerImpression'] = ls[0]['salesPerImpression']
                temp['viewItemPerImpression'] = ls[0]['viewItemPerImpression']
                temp['impressionCount'] = ls[0]['impressionCount']
                groups.append(temp)        
            except Exception, e:
                logger.exception(str(e))
        return groups
    return decorator

@convert_and_check_xml("BESTMATCHAPI")
@aggregatesameitems
def parse_item_bestmatch_data(**kwargs):
        try:
            xml = kwargs.get('xml', None)
            results = []
                
            for singleItem in xml.findall('item'):
                try:
                    result = create_dummy_item_bestmatch()
                    result['itemId'] = long(singleItem.findtext('itemId'))
                    result['itemRank'] = int(singleItem.findtext('itemRank', 0))
                    result['categoryid'] = int(singleItem.findtext('primaryCategory/categoryId'))                    
                    result['salesCount'] = int(singleItem.findtext('bestMatchData/salesCount', 0))
                    result['quantityAvailable'] = int(singleItem.findtext('quantityAvailable', 0))
                    result['quantitySold'] = int(singleItem.findtext('quantitySold', 0))
                    result['viewItemCount'] = int(singleItem.findtext('bestMatchData/viewItemCount', 0))
                    result['salesPerImpression'] = float(singleItem.findtext('bestMatchData/salesPerImpression', 0))
                    result['currentPrice'] = float(singleItem.findtext('sellingStatus/currentPrice', 0))
                    result['viewItemPerImpression'] = float(singleItem.findtext('bestMatchData/viewItemPerImpression', 0))
                    result['impressionCountMin'] = float(singleItem.findtext('bestMatchData/impressionCountRange/min', 0.0));
                    result['impressionCountMax'] = float(singleItem.findtext('bestMatchData/impressionCountRange/max', 0.0));
                    result['impressionCount'] = (result['impressionCountMin'] + result['impressionCountMax']) / 2
                    if singleItem.findtext('bestMatchData'):
                        logger.debug("No best match data found for item %s" % str(result['itemId']))
                    results.append(result)
                except Exception , e:
                    logger.debug(str(e))

            return results 
        except Exception, e:
            logger.exception(str(e))
            raise TransformerException(str(e))
        finally:
            return results 
