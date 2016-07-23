'''
Created on Oct 22, 2011

@author: Mudassar
'''

import unittest
from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
    StringType, DictType, ListType
import json

import os
from etl.tranformers.ebay.findingapi import parse_categoryHistogramContainer, \
    parse_for_items, parse_for_aspects
from etl.tranformers.ebay.tradingapi import parse_trading_api_get_item
from twisted.spread.jelly import DictTypes


class TestEBayExtractionTradingAPI(unittest.TestCase):
    
    def setUp(self):
        self.datapath = os.path.dirname(os.path.abspath(__file__))
        unittest.TestCase.setUp(self)
       

    def test_get_multiple_items_detail(self):
        """
            Test for valid response for officeshoes for get item detail data
        """
        path = os.path.join(self.datapath, "tradingapi_getitem.xml")
        with open(path) as f:
            xml = f.read()
        results = parse_trading_api_get_item(xml=xml)
        self.assertEqual(type(results), dict, "Response in dump should be list")
        self.assertTrue(type(results.get('itemId')) is LongType, "itemId not long in response %s" % results.get('itemId'))
        self.assertTrue(type(results.get('variation')) in (ListType, NoneType), "Variatio not ListType or None type in response %s" % results.get('variation'))
        if type(results.get('variation')) is StringType:
            self.assertTrue(type(json.loads(results.get('variation'))) is ListType, "json itemspecifics not proper json %s" % results.get('itemId'))
        self.assertTrue(type(results.get('convertedCurrentPrice')) in (StringType, NoneType), "convertedCurrentPrice not FloatType or None type in response %s" % results.get('convertedCurrentPrice'))
        if type(results.get('itemspecifics')) is StringType:
            self.assertTrue(type(json.loads(results.get('itemspecifics'))) is DictType, "json itemspecifics not proper json %s" % results.get('itemspecifics'))


            
if __name__ == '__main__':
    unittest.main()

