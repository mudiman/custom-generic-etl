#'''
#Created on Oct 22, 2011
#
#@author: Mudassar
#'''
#
#import etl
#
#from etl.tests.picloud.ebay import TestPiCloudETLDriver, loadfile
#import unittest
#from types import IntType, NoneType, UnicodeType, LongType, FloatType, \
#    StringType, DictType, ListType
#import json
#import cloud
#
#
#class TestPiCloud(unittest.TestCase):
#    
#    def setUp(self):
#        self.etldriver = TestPiCloudETLDriver()
#        unittest.TestCase.setUp(self)
#       
#    def test_get_categories(self):
#        """
#            Test picloud for simple job
#        """
#        jobid=cloud.call(etl.get_items,etldriver=self.etldriver,sellerid='officeshoes',categoryid='11450',locale='GB',limit=100,offset=0,_label="TEST")
#        cloud.join(jobid)
#        status=cloud.status(jobid)
#        self.assertTrue(status=="done", "Job failed on picloud")
#        results=cloud.result(jobid)
#        self.assertEqual(type(results), dict, "Response is not dict")
#    
#    
#            
#if __name__ == '__main__':
#    unittest.main()
