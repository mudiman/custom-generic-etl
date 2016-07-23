'''
Created on Oct 22, 2011

@author: Mudassar
'''

import unittest
from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
    StringType, DictType, ListType
import json
import simplejson
import os
import sys

p = os.path.dirname(os.path.abspath(__file__))

for i in range(0, 5):
    p = os.path.split(p)[0]

sys.path.append(p)


from etl.tranformers.ebay.findingapi import parse_categoryHistogramContainer, \
    parse_for_items, parse_for_aspects


class TestEBayExtractionFindingAPI(unittest.TestCase):
    
    def setUp(self):
        self.datapath = os.path.dirname(os.path.abspath(__file__))
        unittest.TestCase.setUp(self)
       
    def test_get_parse_category_histogram(self):
        """
            Test for valid response for officeshoes for findingitemadvance categoryhistogram
        """
        path = os.path.join(self.datapath, "findingapi_categoryhistogram.xml")
        with open(path) as f:
            xml = f.read()
        result = parse_categoryHistogramContainer(xml=xml, category=11450)
        self.assertEqual(type(result), dict, "Response is not dict")
        self.assertTrue(type(result.get('categoryid')) in (IntType, NoneType), "categoryid not unicode or None type in response")
        self.assertTrue(type(result.get('count')) in (IntType, NoneType), "count not int or None type in response")
        self.assertTrue(type(result.get('categoryName')) in (StringType, NoneType), "categoryName not unicode or None type in response")
        childcategories = result.get('childcategories')
        self.assertEqual(type(childcategories), list, "Response is not list")
        for childcategory in childcategories:
            self.assertTrue(type(childcategory.get('categoryid')) in (IntType, NoneType), "categoryid not unicode or None type in response")
            self.assertTrue(type(childcategory.get('count')) in (IntType, NoneType), "count not int or None type in response")
            self.assertTrue(type(childcategory.get('categoryName')) in (StringType, NoneType), "categoryName not unicode or None type in response")


    def test_get_parse_aspects(self):
        """
            Test for valid response for officeshoes for findingitemadvance categoryhistogram
        """
        path = os.path.join(self.datapath, "findingapi_aspecthistogram.xml")
        with open(path) as f:
            xml = f.read()
        result = parse_for_aspects(xml=xml)
        self.assertEqual(type(result), StringType, "Response is not string")
        result = simplejson.loads(result)
        for key, value in result.items():
            self.assertTrue(type(key) is StringType, "Aspect dict key is not string")
            self.assertTrue(type(value) is DictType, "Aspect value should be dictionary")
            for k, v in value.items():
                self.assertTrue(type(k) is StringType, "Aspect value dict key is not string")
                self.assertTrue(type(v) is IntType, "Aspect value dict value should be int")
        
    
    def test_get_items(self):
        """
            Test for valid response for officeshoes for findingitemadvance items
        """
        with open(os.path.join(self.datapath, "findingapi_items.xml")) as f:
            xml = f.read()
        data = parse_for_items(xml=xml)
        self.assertEqual(type(data), list, "Response in dump should be list")
        for item in data:
            self.assertTrue(type(item.get('itemId')) is LongType, "itemId not long in response %s" % item.get('itemId'))
            self.assertTrue(type(item.get('thumbUrl')) in (StringType, NoneType), "thumbUrl not UnicodeType or None type in response %s" % item.get('thumbUrl'))
            self.assertTrue(type(item.get('convertedCurrentPrice')) in (FloatType, NoneType), "convertedCurrentPrice not FloatType or None type in response %s" % item.get('convertedCurrentPrice'))
            self.assertTrue(type(item.get('galleryPlusPictureURL')) in (StringType, NoneType), "galleryPlusPictureURL not int or None type in response %s" % item.get('galleryPlusPictureURL'))
            self.assertTrue(type(item.get('storename')) in (StringType, NoneType), "storename not int or None type in response %s" % item.get('storename'))
            self.assertTrue(type(item.get('title')) in (StringType, NoneType), "title not UnicodeType or None type in response %s" % item.get('title'))
            self.assertTrue(type(item.get('primarycategoryname')) in (StringType, NoneType), "primarycategoryname not UnicodeType or None type in response %s" % item.get('primarycategoryname'))
            self.assertTrue(type(item.get('categories_categoryid')) in (IntType, NoneType), "categoryName not int or None type in response %s" % item.get('categories_categoryid'))
            self.assertTrue(type(item.get('sellerid')) in (StringType, NoneType), "categoryName not StringTypes or None type in response %s" % item.get('sellerid'))
            self.assertTrue(type(item.get('secondarycategoryname')) in (StringType, NoneType), "secondarycategoryname not UnicodeType or None type in response %s" % item.get('secondarycategoryname'))
            self.assertTrue(type(item.get('viewItemURL')) in (StringType, NoneType), "viewItemURL not UnicodeType or None type in response %s" % item.get('viewItemURL'))



            
if __name__ == '__main__':
    unittest.main()

