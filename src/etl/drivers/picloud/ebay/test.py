'''
Created on Sep 14, 2012

@author: mudassar
'''
import time
import cloud
from etl.config.drivers.picloud import setcloudkey



def testruntime(**kwargs):
    i=0
    while i!=5:
        cloud.cloud.cloudLog.info("test run time")
        print "test run time"
        time.sleep(1)
        i+=1
    
if __name__ == '__main__':
    setcloudkey()
    jobid=cloud.call(testruntime,a=1,_max_runtime=1,_label="TESTRUNTIME")
    cloud.join(jobid)
    print cloud.status(jobid)
    pass