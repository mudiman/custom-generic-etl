from etl.tranformers.generic import TransformersSkeleton
from etl.tranformers.ebay.findingapi import parse_categoryHistogramContainer, \
    parse_for_items, parse_for_aspects
from etl.tranformers.ebay.tradingapi import parse_trading_api_get_item, \
    parse_check_token, parse_get_token_user
from etl.tranformers.ebay.bestmatchapi import parse_item_bestmatch_data
from etl.miscfunctions import create_list, log_exception_message
import itertools
import json
from types import DictType
from etl.hooks.etljobhooks import class_logging_decorator


class EBayTransformers(TransformersSkeleton):
    
    @class_logging_decorator
    def transform_get_categories(self, **kwargs):
        xml = kwargs.get('xml', None)
        categoryid = kwargs.get('categoryid', None)
        sellerid = kwargs.get('sellerid', None)
        selleridkey = kwargs.get('selleridkey', None)
        locale = kwargs.get('locale', None)
        category = parse_categoryHistogramContainer(xml=xml, categoryid=categoryid)
        category['categoryid'] = categoryid
        category['aspect'] = parse_for_aspects(xml=xml)
        category['sellerid'] = selleridkey
        category['isleaf'] = 0
        if len(category['childcategories']) == 0:
            category['isleaf'] = 1
        return category
    
    @class_logging_decorator    
    def transform_get_items(self, **kwargs):
        xml = kwargs.get('xml', None)
        sellerid = kwargs.get('sellerid', None)
        selleridkey = kwargs.get('selleridkey', None)
        categoryid = kwargs.get('categoryid', None)
        locale = kwargs.get('locale', None)
        result = []
        for singlexml in xml:
            try:
                result.append(parse_for_items(xml=singlexml))
            except Exception, e:
                log_exception_message(str(e))
        temp = list(itertools.chain(*result))
        for item in temp:
            item['sellerid'] = selleridkey
            item['categories_categoryid'] = categoryid
        return temp
    
    @class_logging_decorator
    def transform_get_item_detail(self, **kwargs):
        xml = kwargs.get('xml', None)
        temp = create_list(xml)
        result = []
        for singlexml in temp:
            try:
                result.append(parse_trading_api_get_item(xml=singlexml))
            except Exception, e:
                log_exception_message(str(e))
        for item in result:
            if kwargs.get('selleridkey'):
                item['sellerid'] = kwargs.get('selleridkey')
            if item.get('variation'):
                item['quantitySold'] = 0
                item['quantity'] = 0
                for itm in item['variation']: item['quantitySold'] += itm.get('QuantitySold', 0)
                for itm in item['variation']: item['quantity'] += itm.get('Quantity', 0)
            if item['quantity'] == 0:item['quantity'] = 1
            if item['buy_now_price'] == 0.0: 
                item['price'] = item['currentPrice']
            else:
                item['price'] = item['buy_now_price']
            item['inventory_value'] = float(item['price']) * int(item['quantity'])
            item['sales'] = float(item['price']) * int(item['quantitySold'])
            item['variationcount'] = 1
            if len(item.get('variation', [])) > 0:
                item['variationcount'] = len(item.get('variation'))
            if len(item['variation']) > 0:item['variation'] = json.dumps(item['variation'])
            if len(item['itemspecifics']) > 0:item['itemspecifics'] = json.dumps(item['itemspecifics'])
        return result
    
    @class_logging_decorator
    def transform_check_token(self, **kwargs):
        xml = kwargs.get('xml', None)
        result = parse_check_token(xml=xml)
        return result

    @class_logging_decorator
    def transform_check_user_with_token(self, **kwargs):
        xml = kwargs.get('xml', None)
        result = parse_get_token_user(xml=xml)
        return result
    
    @class_logging_decorator
    def transform_get_item_analytics(self, **kwargs):
        xml = kwargs.get('xml', None)
        temp = create_list(xml)
        result = []
        for singlexml in temp:
            try:
                result.append(parse_item_bestmatch_data(xml=singlexml))
            except Exception, e:
                log_exception_message(str(e))
        result = list(itertools.chain(*result))
        if type(result) is DictType:
            if result.get('quantityAvailable'):
                del result['quantityAvailable']
            if result.get('quantitySold'):
                del result['quantitySold']
        for item in result:
            if item.get('quantityAvailable'):
                del item['quantityAvailable']
            if item.get('quantitySold'):
                del item['quantitySold']
            item['conversionrate'] = 0.0
            if item['viewItemCount'] != 0:
                item['conversionrate'] = float(item['salesCount']) / item['impressionCount']
        return result
        
