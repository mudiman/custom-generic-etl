'''
Created on Oct 22, 2011

@author: Mudassar
'''

import unittest
from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
    StringType, DictType, ListType
import json

import os
from etl.tranformers.ebay.bestmatchapi import parse_item_bestmatch_data


class TestEBayExtractionTradingAPI(unittest.TestCase):
    
    def setUp(self):
        self.datapath = os.path.dirname(os.path.abspath(__file__))
        unittest.TestCase.setUp(self)
       

        

    def test_get_multiple_items_analytics(self):
        """
            Test for valid response for officeshoes for bestmatch data
        """

        path = os.path.join(self.datapath, "bestmatchapi_getitemdetails.xml")
        with open(path) as f:
            xml = f.read()        
        data = parse_item_bestmatch_data(xml=xml)
        self.assertEqual(type(data), list, "Response in dump should be list")
        for item in data:
            self.assertTrue(type(item.get('itemId')) is LongType, "itemId not long in response %s" % item.get('itemId'))
            self.assertTrue(type(item.get('viewItemPerImpression')) is FloatType, "viewItemPerImpression not FloatType in response %s" % item.get('viewItemPerImpression'))
            self.assertTrue(type(item.get('quantitySold')) is IntType, "quantitySold not IntType in response %s" % item.get('quantitySold'))
            self.assertTrue(type(item.get('impressionCount')) is FloatType, "impressionCount not FloatType in response %s" % item.get('impressionCount'))
            self.assertTrue(type(item.get('salesPerImpression')) is FloatType, "salesPerImpression not FloatType in response %s" % item.get('salesPerImpression'))
            self.assertTrue(type(item.get('viewItemCount')) is IntType, "viewItemCount not int response %s" % item.get('viewItemCount'))
            self.assertTrue(type(item.get('categoryid')) is IntType, "categoryid not IntType in response %s" % item.get('categoryid'))
            self.assertTrue(type(item.get('salesCount')) is IntType, "salesCount not IntType in response %s" % item.get('salesCount'))

            
if __name__ == '__main__':
    unittest.main()

