from etl.config.loadconfig import COMPANY_CONFIG
import os
import cloud
import logging

config = dict(
            keyid=COMPANY_CONFIG['picloudkey']['etl']['keyid'],
            key=COMPANY_CONFIG['picloudkey']['etl']['key']
            )


def setcloudloglevel():
    cloud.cloud.cloudLog.setLevel(logging.CRITICAL)

def setcloudkey():
    cloud.setkey(config['keyid'], config['key'])

def removelockfile():
    try:
        pass
        print "Remove picloud lock file"
        if os.path.exists("~/.picloud/cloud.lock"):
            os.system("rm ~/.picloud/cloud.lock")
    except Exception, e:
        print e
