'''
Created on Apr 18, 2012

@author: mudassar
'''
import unittest
from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
    StringType, DictType, ListType
import json
from elementtree.ElementTree import fromstring
from etl.extractors.ebay.findingapi import get_categories, get_items
from etl.extractors.ebay.tradingapi import get_item_detail, check_token, \
    token_user
from etl.extractors.ebay.bestmatchapi import get_items_bestmatch_data

def remove_namespace(doc, namespace):
    """
        Remove namespace in the passed document in place.
    """
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]
        

class TestEBayCalls(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
       
    def test_get_categories(self):
        """
            Test for valid response for officeshoes for findingitemadvance categoryhistogram
        """
        xml = get_categories(sellerid='officeshoes', categoryid=11450, locale="GB")
        xml = fromstring(xml)
        namespace = 'http://www.ebay.com/marketplace/search/v1/services'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('ack') == "Success", "Ack node got failure")        
        categoryHistogramContainer = xml.findall('categoryHistogramContainer/categoryHistogram')
        self.assertTrue(len(categoryHistogramContainer) > 0, "Response aspect categoryHistogramContainer is less then zero")
        
        for categoryhistogram in categoryHistogramContainer:
            self.assertTrue(categoryhistogram.findtext('categoryId'), "categoryhistogram has no categoryid node")
            self.assertTrue(categoryhistogram.findtext('categoryName'), "categoryhistogram has no categoryName node")
            self.assertTrue(categoryhistogram.findtext('count'), "categoryhistogram has no count node")
            for childCategoryHistogram in categoryhistogram.findall('childCategoryHistogram'):
                self.assertTrue(childCategoryHistogram.findtext('categoryId'), "categoryhistogram has no categoryid node")
                self.assertTrue(childCategoryHistogram.findtext('categoryName'), "categoryhistogram has no categoryName node")
                self.assertTrue(childCategoryHistogram.findtext('count'), "categoryhistogram has no count node")
    
    
    def test_get_category_aspects(self):
        """
        """
        xml = get_categories(sellerid='officeshoes', categoryid=11498, locale="GB")
        xml = fromstring(xml)
        namespace = 'http://www.ebay.com/marketplace/search/v1/services'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('ack') == "Success", "Ack node got failure")        
        aspectsxml = xml.findall('aspectHistogramContainer/aspect')
        self.assertTrue(len(aspectsxml) > 0, "Response aspect lenght is less then zero")
        for aspect in aspectsxml:
            self.assertTrue(aspect.attrib['name'], "aspect has no attrib name")        
            valueHistograms = aspect.findall('valueHistogram')
            self.assertTrue(len(valueHistograms) > 0, "Response aspect valuehistogram length is less then zero")            
            for valueHistogram in valueHistograms:
                self.assertTrue(valueHistogram.attrib['valueName'], "aspect valuehistogram valuename has no attrib valueName")
                self.assertTrue(valueHistogram.findtext('count'), "aspect valuehistogram  has no count")
            
    def test_get_items(self):
        """
            Test for valid response for officeshoes for findingitemadvance items
        """
        xml = get_items(sellerid='officeshoes', categoryid=11450, locale="GB", entriesPerPage=100)
        xml = fromstring(xml)
        namespace = 'http://www.ebay.com/marketplace/search/v1/services'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('ack') == "Success", "Ack node got failure")
        results = xml.findall('searchResult/item')
        self.assertEqual(type(results), list, "Response is not list")
        self.assertTrue(len(results) > 0, "Item not present when it should be")
        for item in results:
            self.assertTrue(item.findtext('itemId'), "item has no itemId node")
            self.assertTrue(item.findtext('title'), "item has no title node")
            self.assertTrue(item.findtext('galleryURL'), "item has no galleryURL node")
            self.assertTrue(item.findtext('galleryPlusPictureURL'), "item has no galleryPlusPictureURL node")
            self.assertTrue(item.findtext('sellingStatus/currentPrice'), "item has no primaryCategory/categoryId node")
            self.assertTrue(item.findtext('primaryCategory/categoryId'), "item has no itemId node")
            self.assertTrue(item.findtext('primaryCategory/categoryName'), "item has no primaryCategory/categoryName node")
            self.assertTrue(item.findtext('sellingStatus/convertedCurrentPrice'), "item has no sellingStatus/convertedCurrentPrice node")
            self.assertTrue(item.findtext('storeInfo/storeName'), "item has no storeInfo/storeName node")
            self.assertTrue(item.findtext('viewItemURL'), "item has no viewItemURL node")
            

    def test_get_multiple_items_detail(self):
        """
            Test for valid response for officeshoes for get item detail data
        """
        xml = get_item_detail(itemid=130531566351)
        xml = fromstring(xml)
        namespace = 'urn:ebay:apis:eBLBaseComponents'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('Ack') == "Success", "Ack node got failure")

        self.assertTrue(type(xml.findtext('Item/ItemID')) is StringType, "Response node Item/ItemID should be string")
        self.assertTrue(type(xml.findtext('Item/PictureDetails/GalleryURL')) is StringType, "Response node Item/PictureDetails/GalleryURL should be string")
        self.assertTrue(type(xml.findtext('Item/PictureDetails/PhotoDisplay')) is StringType, "Response node Item/PictureDetails/PhotoDisplay should be string")
        
        itemspecifics = xml.findall('Item/ItemSpecifics/NameValueList')
        self.assertTrue(len(itemspecifics) > 0, "There should be item specifics but got none")        
        for tt in itemspecifics:
            self.assertTrue(tt.findtext('Name'), "item specific node has no Name node")
            self.assertTrue(tt.findtext('Value'), "item specific node has no Value node")
        variations = xml.findall('Item/Variations/Variation')
        self.assertTrue(len(variations) > 0, "There should be variations but got none")
        for variation in variations:
            self.assertTrue(variation.findtext('SKU'), "item variation node has no Name node")
            self.assertTrue(variation.findtext('Quantity'), "item variation node has no Quantity node")
            self.assertTrue(variation.findtext('SellingStatus/QuantitySold'), "item variation node has no SellingStatus/QuantitySold node")
            variationnamevalue = variation.findall('VariationSpecifics/NameValueList')
            self.assertTrue(len(variationnamevalue) > 0, "There should be Name value list for each variation but got none")
            for subSpe in variationnamevalue:
                self.assertTrue(subSpe.findtext('Name'), "item specific node has no Name node")
                self.assertTrue(subSpe.findtext('Value'), "item specific node has no Value node")
   
                    
    def test_check_token(self):
        """
            Test for valid response for officeshoes for get item detail data
        """
        xml = check_token("AgAAAA**AQAAAA**aAAAAA**fAKrTA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AEkYWmDpGFowidj6x9nY+seQ**e8IAAA**AAMAAA**tffEcQxfrIgX54HR4wY2wmBbvpC+9DsqfFdrNzmpXAzTE+CXC+OOIFsWUYpzjoyT6ILhG9IlUYV74PdsJtOWVIRyBvvpaHdNb/4C+gPSCu9NTdk552eTI7ioqgVjvCLVr10Fw8StkIK+IS4WlL2LBINat1n2qZH+ySc8ktFCktp8rGHUMfn4m97Vl3hjxPJVKIdB5zdHwy0uLh/cKFURy6FeMeJGNZdBKnYpO1znBzZ9gILVngAPlJofggkPWYa4NFQ7dFY+ylY2VQpPBkEz/t4ZCMT+u9AqiRTrjIclDvmE9Z5TkELXJGVSIxPCQe1WqspLndwCPlm2Nd1h9GrXqFElIkFWFLmXPbOpTZ6ZT81rGusasxI3GwyytjnRKpR8WqUhc20aU0ZD1EnSfGMjHDyE5fPef1LXN0DKXCmz8QIsrZKkb9R1DJYGomK7toCfKGUry3VCmtuiNrH116OS+6fUmUyAC73a93Sb/4ZH3RloYCCJnZtOQF+/WqomCyOR5CAzXMeuXIRRKfKfuaek40H+g5h4CsGLuYooesEounOGQTkNk496fmSfZh+pK/ch8w1Mxu450j0HwD+olnVbNeKy0OIHt9HcHo7QjQwr25wE6G+TPpBK3yr9J0nORN5IQI8uDccFxABYeCFTLuAPMA2foH84gOoN0nfSSGt2U5Hl3LH8rwkU05o+1HCIGI14CpWhDYq5gTIcv2jHzIqnnjqDGZg4h0DcNAf6opoCkAEpAwKzWvHUciKTDaC77K3o")
        xml = fromstring(xml)
        namespace = 'urn:ebay:apis:eBLBaseComponents'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('Ack') == "Success", "Ack node got failure")
        self.assertTrue(xml.findtext('TokenStatus/Status') == "Active", "TokenStatus/Status node for active value")

    def test_token_user(self):
        """
            Test for valid response for officeshoes for get item detail data
        """
        xml = token_user("AgAAAA**AQAAAA**aAAAAA**fAKrTA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AEkYWmDpGFowidj6x9nY+seQ**e8IAAA**AAMAAA**tffEcQxfrIgX54HR4wY2wmBbvpC+9DsqfFdrNzmpXAzTE+CXC+OOIFsWUYpzjoyT6ILhG9IlUYV74PdsJtOWVIRyBvvpaHdNb/4C+gPSCu9NTdk552eTI7ioqgVjvCLVr10Fw8StkIK+IS4WlL2LBINat1n2qZH+ySc8ktFCktp8rGHUMfn4m97Vl3hjxPJVKIdB5zdHwy0uLh/cKFURy6FeMeJGNZdBKnYpO1znBzZ9gILVngAPlJofggkPWYa4NFQ7dFY+ylY2VQpPBkEz/t4ZCMT+u9AqiRTrjIclDvmE9Z5TkELXJGVSIxPCQe1WqspLndwCPlm2Nd1h9GrXqFElIkFWFLmXPbOpTZ6ZT81rGusasxI3GwyytjnRKpR8WqUhc20aU0ZD1EnSfGMjHDyE5fPef1LXN0DKXCmz8QIsrZKkb9R1DJYGomK7toCfKGUry3VCmtuiNrH116OS+6fUmUyAC73a93Sb/4ZH3RloYCCJnZtOQF+/WqomCyOR5CAzXMeuXIRRKfKfuaek40H+g5h4CsGLuYooesEounOGQTkNk496fmSfZh+pK/ch8w1Mxu450j0HwD+olnVbNeKy0OIHt9HcHo7QjQwr25wE6G+TPpBK3yr9J0nORN5IQI8uDccFxABYeCFTLuAPMA2foH84gOoN0nfSSGt2U5Hl3LH8rwkU05o+1HCIGI14CpWhDYq5gTIcv2jHzIqnnjqDGZg4h0DcNAf6opoCkAEpAwKzWvHUciKTDaC77K3o")
        xml = fromstring(xml)
        namespace = 'urn:ebay:apis:eBLBaseComponents'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('Ack') == "Success", "Ack node got failure")
        self.assertTrue(xml.findtext('User/UserID') == "company_test", "User/UserID node for company_test value")
               
    def test_get_multiple_items_analytics(self):
        """
            Test for valid response for officeshoes for bestmatch data
        """
        token = 'AgAAAA**AQAAAA**aAAAAA**5XalTQ**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wBkISmCpKGpQ2dj6x9nY+seQ**e8IAAA**AAMAAA**gGyOR7+2j3LDKDyi71k34bfoFk6AQgIZ5LVIsPuUFnqfBua1/sb4VDLXeovINnWEeg6jEJgjnD7akKId38j23A54A4vfkg3nggilz+tJHNZIx8ZV6s8UZvZqgA696Lbwqz61eiImZ7KZeFjeS5BsvHam38acaSqbMj8fu3UHdcd+F15NNqXwEdXOLCO34I+TkAontsGLDo/VZM87adUDLU80t7m7dCzkLqsmQbN9a7XVQTn0bNdM/I58tyVnaQ8wvRGd4EF2Kkd38Ksm61mLsY4u67vcFeTjIUBuiI3foyfit64T0Zy2RbgVgTq3q1tK479fabdBHvWeDEW7yKhfJ60hLkJ4b0THnkz2xN6cXkp84YYi4xvQfruyRBsuR7156eLMfpqd6vlgYdXrds01PyUJWpTX1gNwu4UCTg1gHXCpNMoOUmQNhhSefApk1HeOA6ziYOfq0LwuW5kQEDNxcOuh1FCHnCwd4VEj9YqBBYQVsAz7utFO/05Qsxd2WbqJGlFh0b4JioVZgSbcwBFvdd5fSbLUDwGcw8rl+SPPpfCenk2HQsLKdFPI3QC9y4rFW6LUW5dM+fjA4tHKGzbQpIM1ofF2VmnfEpmPCwxAvF0N6BX5SqSDdO1gQf7GWATjy3i3fz2NViE79f7xEEdA+ShgjQ6pZVExo7WwF+R1tEo3rupwYCZ6eNPfV0C3OW3LtHLpOyqY3foZegV1CnGY04IiGgd1UxS6BJC/oYtEU7iJhArEatLD8soYHddMz0Q0'
        locale = 'GB'
        xml = get_items_bestmatch_data(locale=locale, token=token, itemid='<itemId>130531566351</itemId>')
        xml = fromstring(xml)
        namespace = 'http://www.ebay.com/marketplace/search/v1/services'
        remove_namespace(xml, namespace)
        self.assertTrue(xml.findtext('ack') == "Success", "Ack node got failure")
        results = xml.findall('item')
        self.assertEqual(type(results), list, "Response is not list")
        self.assertTrue(len(results) > 0, "Item not present when it should be")
        for item in results:
            self.assertTrue(item.findtext('itemId'), "item has no itemId node")
            self.assertTrue(item.findtext('itemRank'), "item has no itemRank node")
            self.assertTrue(item.findtext('primaryCategory/categoryId'), "item has no primaryCategory/categoryId node")
            self.assertTrue(item.findtext('bestMatchData/salesCount'), "item has no bestMatchData/salesCount node")
            self.assertTrue(item.findtext('quantityAvailable'), "item has no quantityAvailable node")
            self.assertTrue(item.findtext('quantitySold'), "item has no quantitySold node")
            self.assertTrue(item.findtext('bestMatchData/viewItemCount'), "item has no bestMatchData/viewItemCount node")
            self.assertTrue(item.findtext('bestMatchData/salesPerImpression'), "item has no bestMatchData/salesPerImpression node")
            self.assertTrue(item.findtext('sellingStatus/currentPrice'), "item has no sellingStatus/currentPrice node")
            self.assertTrue(item.findtext('bestMatchData/viewItemPerImpression'), "item has no bestMatchData/viewItemPerImpression node")
            self.assertTrue(item.findtext('bestMatchData/impressionCountRange/min'), "item has no bestMatchData/impressionCountRange/min node")
            self.assertTrue(item.findtext('bestMatchData/impressionCountRange/max'), "item has no bestMatchData/impressionCountRange/max node")

            
if __name__ == '__main__':
    unittest.main()

