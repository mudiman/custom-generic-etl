import sys
from etl.config.drivers import PICLOUDEBAY





try:
    import etl
except ImportError:
    import os

    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)




from etl.config.loader.mysql import Connection

from etl.dal import get_seller_by_id
from etl.config.drivers.picloud.ebay import picloud_ebay_configuration
from etl.config.drivers.threadimp.ebay import threaded_ebay_configuration

from etl.drivers.threadimp.ebay.main import ThreadedEBayBuildIndex
from etl.drivers.picloud.ebay.main import PiCloudEBayBuildIndex
from etl.hooks import pre_driver, post_driver
from etl.config import logger, config


driverlist = dict(
                  picloudebay=dict(driver=PiCloudEBayBuildIndex, configuration=picloud_ebay_configuration),
                  threadedebay=dict(driver=ThreadedEBayBuildIndex, configuration=threaded_ebay_configuration),
                  )




@pre_driver
@post_driver
def start_driver(*args, **kwargs):
    try:
        driverins = kwargs.get('driverins')
        driverins = driverlist[driverins]
        sellerdatalist = kwargs.get('sellerdatalist', [])
        etlconfiguration = kwargs.get('etlconfiguration', "ALL")
        
        driver = driverins['driver'](driverins['configuration'])
        return driver.start_index(sellerdatalist, etlconfiguration)
    except Exception, e:
        logger.exception(str(e))
    

if __name__ == "__main__":
    sellerdatalist = []
    try:
        
        config(environment=sys.argv[2])
        Connection(environment=config.ENVIRONMENT)
        for i in range(3, len(sys.argv)):
            temp = get_seller_by_id(sys.argv[i])
            if temp:
                sellerdatalist.append(temp)
            else:
                print "seller not available in db %s " % sys.argv[i]
        
        start_driver(driverins=sys.argv[1], sellerdatalist=sellerdatalist)
        
    except Exception, e:
        print e
        print """please specify all argument drivers name plus source of extraction\
        \nfor example\n "python path/driver """+PICLOUDEBAY+""" ENVIRONMENT sellerid1,sellerid2" """
        sys.exit()   
    finally:
        Connection(config.ENVIRONMENT).mysql_connection.close()
        sys.exit()
        os.kill(os.getpid(), 9)
    
    
