'''
Created on Feb 20, 2012

@author: Mudassar Ali
'''
import os
import json
import sys


try:
    if not os.environ.get('COMPANY_CONFIG'):
        COMPANY_CONFIG = {}
    else:
        with open(os.environ['COMPANY_CONFIG'], "r") as conf_file:
            data = conf_file.read()
            COMPANY_CONFIG = json.loads(data)
except Exception, e:
    print "Environment configuration error " + str(e)
