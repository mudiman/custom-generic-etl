'''
Created on Oct 31, 2011

@author: Mudassar Ali
'''
import ConfigParser
'''
Created on Sep 26, 2011

@author: Mudassar Ali
'''
"""
This file will run as root
"""

import os

ETL_DEPLOYMENT_PATH = os.environ['WORKSPACE']



def run_system_command(cmd, prn=True):
    if prn == True:
        print cmd
    try:
        os.system(cmd)
    except Exception, e:
        print e
    
def updatePaths():
    pass
#    config = ConfigParser.RawConfigParser()
#    config.add_section('solrpath')
#    config.set('solrpath', 'string', 'http://192.168.85.56:8888/apache-solr-3.4.0')
#    config.add_section('itemserviceurl')
#    config.set('itemserviceurl', 'string', 'http://192.168.85.56/company-items-service')
#    config.add_section('scoringserviceurl')
#    config.set('scoringserviceurl', 'string', 'http://192.168.85.56/company-scoring-service')
#    config.add_section('rootkey')
#    config.set('rootkey', 'string', 'SovoiaKey MjU1MjE5ODQwMjY0ODM3NzI2OTEyNjIyMzM1MjU21C8V0O8M51C8V0O8M5')
#    config.add_section('environment')
#    config.set('environment', 'string', 'TEST')
#    
#    with open("%ssrc/etl/config/config.cfg" % ETL_DEPLOYMENT_PATH, 'wb') as configfile:
#        config.write(configfile)

        
if __name__ == '__main__':
    updatePaths()
    run_system_command("chmod 777 -R %s/src/etl/config/*.log" % ETL_DEPLOYMENT_PATH)
    run_system_command("chmod 777 -R %s/src/etl/loaders" % ETL_DEPLOYMENT_PATH)   

    
