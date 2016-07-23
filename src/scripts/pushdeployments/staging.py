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

STAGGING_SERVICE_PATH = "/var/www/html/test1/etl/"

p = os.path.dirname(os.path.abspath(__file__))

for i in range(0, 2):
    p = os.path.split(p)[0]
p = os.path.join(p, "etl")

ETL_STAGGING_DEPLOYMENT_PATH = p


def run_system_command(cmd, prn=True):
    if prn == True:
        print cmd
    os.system(cmd)
    
def updatePaths():
    pass
#    config = ConfigParser.RawConfigParser()
#    config.add_section('solrpath')
#    config.set('solrpath', 'string', 'http://staging.company.com:8080/apache-solr-3.4.0')
#    config.add_section('itemserviceurl')
#    config.set('itemserviceurl', 'string', 'http://staging.company.com/company-items-service')
#    config.add_section('scoringserviceurl')
#    config.set('scoringserviceurl', 'string', 'http://staging.company.com/company-scoring-service')
#    config.add_section('rootkey')
#    config.set('rootkey', 'string', 'SovoiaKey MjU1MjE5ODQwMjY0ODM3NzI2OTEyNjIyMzM1MjU21C8V0O8M51C8V0O8M5')
#    config.add_section('environment')
#    config.set('environment', 'string', 'TEST')
#
#    with open("%ssrc/etl/config/config.cfg" % STAGGING_SERVICE_PATH, 'wb') as configfile:
#        config.write(configfile)
if __name__ == '__main__':       
    updatePaths()       
    run_system_command("chmod 777 -R %s/config/*.log" % ETL_STAGGING_DEPLOYMENT_PATH)
    run_system_command("chmod 777 -R %ssrc/etl/loaders" % ETL_STAGGING_DEPLOYMENT_PATH)   
    
    
