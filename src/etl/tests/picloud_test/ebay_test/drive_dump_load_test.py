'''
Created on Oct 22, 2011

@author: Mudassar
'''
import etl

from etl.tests.picloud_test.ebay_test import TestPiCloudETLDriver, loadfile
import unittest
from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
    StringType, DictType, ListType
import json


class TestPiCloudEBayETL(unittest.TestCase):
    
    def setUp(self):
        self.etldriver = TestPiCloudETLDriver()
        unittest.TestCase.setUp(self)
       
    def test_get_categories(self):
        """
            Test for valid response for officeshoes for findingitemadvance categoryhistogram
        """
        results = etl.get_categories(etldriver=self.etldriver, sellerid='officeshoes', categoryid=11498, locale="GB")
        self.assertEqual(type(results), dict, "Response is not dict")
        self.assertTrue(type(results.get('count')) in (IntType, NoneType), "count not int or None type in response")
        self.assertTrue(type(results.get('categoryName')) in (UnicodeType, NoneType), "categoryName not unicode or None type in response")
        self.assertTrue(type(results.get('parentcategoryid')) in (IntType, NoneType), "parentcategoryid not int or None type in response")
        self.assertTrue(type(results.get('categoryName')) in (UnicodeType, NoneType), "categoryName not int or None type in response")
        self.assertTrue(type(results.get('aspect')) in (StringType, NoneType), "aspect not int or None type in response")
        if type(results.get('aspect')) is StringType:
                self.assertTrue(type(json.loads(results.get('aspect'))) is DictType, "json aspect not proper json")
    
    def test_get_items(self):
        """
            Test for valid response for officeshoes for findingitemadvance items
        """
        results = etl.get_items(etldriver=self.etldriver, sellerid='calvinklein', selleridkey="__BLANK_COMPANY_SELLER__@US", categoryid=55793, locale="GB")
        self.assertEqual(type(results), list, "Response is not list")
        for result in results:
            self.assertEqual(type(result), long, "Response item is not long")
        
        data = loadfile("file_aceparts_uk_GB_57364__1000_0_item.txt")
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


    def test_get_multiple_items_detail(self):
        """
            Test for valid response for officeshoes for get item detail data
        """
        results = etl.get_item_details(etldriver=self.etldriver, itemid=[130531566351, 130531567355, 130531567963])
        self.assertEqual(results, None, "Response should be none")
        
        data = loadfile("file____350550055710___itemdetail.txt")
        self.assertEqual(type(data), list, "Response in dump should be list")
        for results in data:
            self.assertTrue(type(results.get('itemId')) is LongType, "itemId not long in response %s" % results.get('itemId'))
            self.assertTrue(type(results.get('variation')) in (ListType, NoneType), "Variatio not ListType or None type in response %s" % results.get('variation'))
            if type(results.get('variation')) is StringType:
                self.assertTrue(type(json.loads(results.get('variation'))) is ListType, "json itemspecifics not proper json %s" % results.get('itemId'))
            self.assertTrue(type(results.get('convertedCurrentPrice')) in (StringType, NoneType), "convertedCurrentPrice not FloatType or None type in response %s" % results.get('convertedCurrentPrice'))
            if type(results.get('itemspecifics')) is StringType:
                self.assertTrue(type(json.loads(results.get('itemspecifics'))) is DictType, "json itemspecifics not proper json %s" % results.get('itemspecifics'))
                    

    def test_get_multiple_items_analytics(self):
        """
            Test for valid response for officeshoes for bestmatch data
        """
        token = 'AgAAAA**AQAAAA**aAAAAA**5XalTQ**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wBkISmCpKGpQ2dj6x9nY+seQ**e8IAAA**AAMAAA**gGyOR7+2j3LDKDyi71k34bfoFk6AQgIZ5LVIsPuUFnqfBua1/sb4VDLXeovINnWEeg6jEJgjnD7akKId38j23A54A4vfkg3nggilz+tJHNZIx8ZV6s8UZvZqgA696Lbwqz61eiImZ7KZeFjeS5BsvHam38acaSqbMj8fu3UHdcd+F15NNqXwEdXOLCO34I+TkAontsGLDo/VZM87adUDLU80t7m7dCzkLqsmQbN9a7XVQTn0bNdM/I58tyVnaQ8wvRGd4EF2Kkd38Ksm61mLsY4u67vcFeTjIUBuiI3foyfit64T0Zy2RbgVgTq3q1tK479fabdBHvWeDEW7yKhfJ60hLkJ4b0THnkz2xN6cXkp84YYi4xvQfruyRBsuR7156eLMfpqd6vlgYdXrds01PyUJWpTX1gNwu4UCTg1gHXCpNMoOUmQNhhSefApk1HeOA6ziYOfq0LwuW5kQEDNxcOuh1FCHnCwd4VEj9YqBBYQVsAz7utFO/05Qsxd2WbqJGlFh0b4JioVZgSbcwBFvdd5fSbLUDwGcw8rl+SPPpfCenk2HQsLKdFPI3QC9y4rFW6LUW5dM+fjA4tHKGzbQpIM1ofF2VmnfEpmPCwxAvF0N6BX5SqSDdO1gQf7GWATjy3i3fz2NViE79f7xEEdA+ShgjQ6pZVExo7WwF+R1tEo3rupwYCZ6eNPfV0C3OW3LtHLpOyqY3foZegV1CnGY04IiGgd1UxS6BJC/oYtEU7iJhArEatLD8soYHddMz0Q0'
        locale = 'GB'
        results = etl.get_item_analytics(etldriver=self.etldriver, itemid=[130531566351, 130531567355, 130531567963], locale=locale, token=token)
        self.assertEqual(results, None, "Response should be none")
        
        data = loadfile("file__GB__330571449794___bestmatch.txt")
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

