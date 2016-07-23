'''
Created on Jan 26, 2012

@author: Mudassar
'''

import os
import json
import cloud
from etl.config.drivers.picloud import config


def sum():
    return 10 + 10


if __name__ == '__main__':
    
    cloud.setkey(config['keyid'], config['key'])
    
    print cloud.realtime.request('c1', 10)

    jobid = cloud.call(sum, _type="c1", _label="TEST")
    
    cloud.join(jobid)
    
    print cloud.result(jobid)
    
