#import unittest
#from etl.tests.picloud.ebay.test import TestPiCloudEBayETL
#from etl.tests.picloud.cloudtest import TestPiCloud
#
#
#def run_create_test_suit():
#    print "Testing basic ETL atomic functions"
#    suite = unittest.TestLoader().loadTestsFromTestCase(TestPiCloudEBayETL)
#    unittest.TextTestRunner(verbosity=2).run(suite)
#    
#    print "Testing Picloud for single atomic functions"
#    suite = unittest.TestLoader().loadTestsFromTestCase(TestPiCloudEBayETL,TestPiCloud)
#    unittest.TextTestRunner(verbosity=2).run(suite)

import nose

def runtests():
    result = nose.run()
    
if __name__ == "__main__":runtests()
    
    
    
