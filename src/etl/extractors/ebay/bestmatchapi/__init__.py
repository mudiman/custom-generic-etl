'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''
from httplib2 import Http
from etl.miscfunctions import cache_raw_data, httpcallswrapper
from etl.config import logger
from etl.exceptions import ExtractorException



@cache_raw_data
def get_items_bestmatch_data(locale, token, itemid):
    """
        Gets ebay bestmatchapi getitemdetails from list of items with itemid
    """
    try:
        rawbody = ""             
        payload = """<?xml version="1.0" encoding="utf-8"?><getBestMatchItemDetailsRequest xmlns="http://www.ebay.com/marketplace/search/v1/services">%s</getBestMatchItemDetailsRequest>""" % itemid

        headers = {'X-EBAY-SOA-SERVICE-NAME':'BestMatchItemDetailsService',
               'X-EBAY-SOA-OPERATION-NAME':'getBestMatchItemDetails',
               'X-EBAY-SOA-SECURITY-TOKEN':token,
               'X-EBAY-SOA-SERVICE-VERSION':'1.5.0',
               'X-EBAY-SOA-GLOBAL-ID':'EBAY-' + locale}
        try:
            responseheaders, rawbody = httpcallswrapper(url='https://svcs.ebay.com/services/search/BestMatchItemDetailsService/v1', method='POST', body=payload, headers=headers)
        except Exception, e:
            logger.debug(str(e))
            raise ExtractorException("Exception from ebay response %s" % str(e))
    except ExtractorException:
        raise
    except Exception , e:
        logger.exception(str(e))
        raise
    finally:
        return rawbody

if __name__ == '__main__':
    pass
    token = 'AgAAAA**AQAAAA**aAAAAA**5XalTQ**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wBkISmCpKGpQ2dj6x9nY+seQ**e8IAAA**AAMAAA**gGyOR7+2j3LDKDyi71k34bfoFk6AQgIZ5LVIsPuUFnqfBua1/sb4VDLXeovINnWEeg6jEJgjnD7akKId38j23A54A4vfkg3nggilz+tJHNZIx8ZV6s8UZvZqgA696Lbwqz61eiImZ7KZeFjeS5BsvHam38acaSqbMj8fu3UHdcd+F15NNqXwEdXOLCO34I+TkAontsGLDo/VZM87adUDLU80t7m7dCzkLqsmQbN9a7XVQTn0bNdM/I58tyVnaQ8wvRGd4EF2Kkd38Ksm61mLsY4u67vcFeTjIUBuiI3foyfit64T0Zy2RbgVgTq3q1tK479fabdBHvWeDEW7yKhfJ60hLkJ4b0THnkz2xN6cXkp84YYi4xvQfruyRBsuR7156eLMfpqd6vlgYdXrds01PyUJWpTX1gNwu4UCTg1gHXCpNMoOUmQNhhSefApk1HeOA6ziYOfq0LwuW5kQEDNxcOuh1FCHnCwd4VEj9YqBBYQVsAz7utFO/05Qsxd2WbqJGlFh0b4JioVZgSbcwBFvdd5fSbLUDwGcw8rl+SPPpfCenk2HQsLKdFPI3QC9y4rFW6LUW5dM+fjA4tHKGzbQpIM1ofF2VmnfEpmPCwxAvF0N6BX5SqSDdO1gQf7GWATjy3i3fz2NViE79f7xEEdA+ShgjQ6pZVExo7WwF+R1tEo3rupwYCZ6eNPfV0C3OW3LtHLpOyqY3foZegV1CnGY04IiGgd1UxS6BJC/oYtEU7iJhArEatLD8soYHddMz0Q0'
    print get_items_bestmatch_data("GB", token, "<itemId>130533057299</itemId>")
