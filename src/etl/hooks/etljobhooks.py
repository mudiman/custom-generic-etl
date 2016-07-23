'''
Created on Sep 14, 2012

@author: mudassar
'''
from etl.miscfunctions import log_critical_message
from etl.config import logger
from etl.config.drivers import THREADEDEBAY


#def log_etl_core_job_message(func):
#    etldriver=None
#    def wrapped(*args, **kwargs):
#        etldriver = kwargs['etldriver']
#        log_critical_message(func.__name__+" start "+str(kwargs),etldriver.customlogger)
#        return func(*args, **kwargs)
#    log_critical_message(func.__name__+" end "+str(kwargs),etldriver.customlogger)
#    return wrapped


class dummylogclass(object):
    def __init__(self):
        self.customlogger=THREADEDEBAY

def class_logging_decorator(func):

    def decorator(self, *args,**kwargs):
        etldriver = kwargs.get('etldriver',dummylogclass())
        if etldriver.customlogger==THREADEDEBAY:
            logger.info(self.__class__.__name__+"  "+func.__name__+" start "+str(kwargs))
        else:
            print self.__class__.__name__+"  "+func.__name__+" start "+str(kwargs)
        resp = func(self, *args, **kwargs)
        if etldriver.customlogger==THREADEDEBAY:
            logger.info(self.__class__.__name__+"  "+func.__name__+" end "+str(kwargs))
        else:
            print self.__class__.__name__+"  "+func.__name__+" end "+str(kwargs)
        return resp
    return decorator            
