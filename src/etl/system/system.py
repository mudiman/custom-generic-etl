'''
Created on Mar 14, 2012

@author: mudassar
'''
from etl.config import logger, config, DUMPS_TIMELIMIT_TO_DELETE_IN_DAYS
from etl.config.loadconfig import COMPANY_CONFIG
import os
import datetime
from etl.config.loader.mysql import Connection
import shutil



def delete_oldfiles_directory_from_path(dir_to_search):
    for dirpath, dirnames, filenames in os.walk(dir_to_search):
        for direc in dirnames:
            curpath = os.path.join(dirpath, direc)
            file_modified = datetime.datetime.fromtimestamp(os.path.getmtime(curpath))
            if datetime.datetime.now() - file_modified > datetime.timedelta(days=DUMPS_TIMELIMIT_TO_DELETE_IN_DAYS) and direc.find(".py")<0:
                try:
                    shutil.rmtree(curpath)
                except Exception,e:
                    print e
        for file in filenames:
            curpath = os.path.join(dirpath, file)
            file_modified = datetime.datetime.fromtimestamp(os.path.getmtime(curpath))
            if datetime.datetime.now() - file_modified > datetime.timedelta(days=DUMPS_TIMELIMIT_TO_DELETE_IN_DAYS) and file.find(".py")<0:
                try:
                    os.remove(curpath)
                except Exception,e:
                    print e
                
def initialize(Environment,drivertype=None):
        config(environment=Environment,drivertype=None)
        Connection(environment=config.ENVIRONMENT)   


def restart_tomcat():
    try:
        pass
#        if COMPANY_CONFIG.get('tomcat-restart'):
#            try:
#                os.system(COMPANY_CONFIG['tomcat-restart'][0])
#            except Exception,e:
#                pass
#            finally:
#                os.system(COMPANY_CONFIG['tomcat-restart'][1])
                
    except Exception, e:
        logger.exception(str(e))
        print e
        
def resetwebservers(apache=True, tomcat=True):
    try:
        pass
#        if COMPANY_CONFIG.get('tomcat-restart'):
#            pass
            #os.system(COMPANY_CONFIG['apache-restart'])
    except Exception, e:
        logger.exception(str(e))
        print e
