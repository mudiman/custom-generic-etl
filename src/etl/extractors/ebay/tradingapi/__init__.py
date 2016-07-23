# coding: utf-8
'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''
from httplib2 import Http
from etl.exceptions import ExtractorException
from etl.config import logger
from etl.miscfunctions import cache_raw_data, httpcallswrapper
from etl.config.extractor.ebay import config


    
@cache_raw_data
def get_item_detail(itemid):
    """
        Gets ebay TradingAPi GetItem item detail response from single item
    """
    try:
        rawbody = ""
        url = 'https://api.ebay.com/ws/api.dll'
        payload = """<?xml version="1.0" encoding="utf-8"?><GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <RequesterCredentials>
            <eBayAuthToken>%s</eBayAuthToken>
            </RequesterCredentials>
            <ItemID>%s</ItemID>
            <IncludeItemSpecifics>True</IncludeItemSpecifics>
            <DetailLevel>ReturnAll</DetailLevel>
            <WarningLevel>High</WarningLevel>
            </GetItemRequest>""" % (config['token'], itemid)

        headers = {'X-EBAY-API-COMPATIBILITY-LEVEL':config['compatibilitylevel'],
           'X-EBAY-API-DEV-NAME':config['devid'],
           'X-EBAY-API-APP-NAME':config['appid'],
           'X-EBAY-API-CERT-NAME':config['certid'],
           'X-EBAY-API-SITEID':'0',
           'X-EBAY-API-CALL-NAME':'GetItem'}
        
        try:
            responseheaders, rawbody = httpcallswrapper(url=url, method="POST", body=payload, headers=headers)
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
            


def check_dev_token(token):
        try:
            payload = """<?xml version="1.0" encoding="utf-8"?>
<GetTokenStatusRequest xmlns="urn:ebay:apis:eBLBaseComponents">
<RequesterCredentials>
<eBayAuthToken>%s</eBayAuthToken>
</RequesterCredentials>
</GetTokenStatusRequest>​""" % token

            headers = {'X-EBAY-API-COMPATIBILITY-LEVEL':config['compatibilitylevel'],
           'X-EBAY-API-DEV-NAME':config['devid'],
           'X-EBAY-API-APP-NAME':config['appid'],
           'X-EBAY-API-CERT-NAME':config['certid'],
           'X-EBAY-API-SITEID':'0',
           'X-EBAY-API-CALL-NAME':'GetTokenStatus'}
            
            responseheaders, rawbody = httpcallswrapper(url='https://api.ebay.com/ws/api.dll', method='POST', body=payload, headers=headers)
            return rawbody
        except Exception, e:
            logger.exception(str(e))
            
def check_token(token):
        try:
            payload = """<?xml version="1.0" encoding="utf-8"?>
<GetTokenStatusRequest xmlns="urn:ebay:apis:eBLBaseComponents">
<RequesterCredentials>
<eBayAuthToken>%s</eBayAuthToken>
</RequesterCredentials>
</GetTokenStatusRequest>​""" % token

            headers = {'X-EBAY-API-COMPATIBILITY-LEVEL':config['compatibilitylevel'],
           'X-EBAY-API-DEV-NAME':'f616c65b-c3d9-4a6f-9c7b-4bd34998e062',
           'X-EBAY-API-APP-NAME':'LMKR52213-43c0-474b-baeb-ea075aaaf3f',
           'X-EBAY-API-CERT-NAME':'17188d14-e6e8-43e6-aecd-6086aea73f50',
           'X-EBAY-API-SITEID':'0',
           'X-EBAY-API-CALL-NAME':'GetTokenStatus'}
            
            responseheaders, rawbody = httpcallswrapper(url='https://api.ebay.com/ws/api.dll', method='POST', body=payload, headers=headers)
            return rawbody
        except Exception, e:
            logger.exception(str(e))
            
def token_user(token):
        try:
            payload = """<?xml version="1.0" encoding="utf-8"?>
<GetUserRequest xmlns="urn:ebay:apis:eBLBaseComponents">
<RequesterCredentials>
<eBayAuthToken>%s</eBayAuthToken>
</RequesterCredentials>
</GetUserRequest>​""" % token

            headers = {'X-EBAY-API-COMPATIBILITY-LEVEL':config['compatibilitylevel'],
           'X-EBAY-API-DEV-NAME':'f616c65b-c3d9-4a6f-9c7b-4bd34998e062',
           'X-EBAY-API-APP-NAME':'LMKR52213-43c0-474b-baeb-ea075aaaf3f',
           'X-EBAY-API-CERT-NAME':'17188d14-e6e8-43e6-aecd-6086aea73f50',
           'X-EBAY-API-SITEID':'0',
           'X-EBAY-API-CALL-NAME':'GetUser'}
            
            responseheaders, rawbody = httpcallswrapper(url='https://api.ebay.com/ws/api.dll', method='POST', body=payload, headers=headers)
            return rawbody
        except Exception, e:
            logger.exception(str(e))

if __name__ == '__main__':
    pass
    print token_user("AgAAAA**AQAAAA**aAAAAA**fAKrTA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AEkYWmDpGFowidj6x9nY+seQ**e8IAAA**AAMAAA**tffEcQxfrIgX54HR4wY2wmBbvpC+9DsqfFdrNzmpXAzTE+CXC+OOIFsWUYpzjoyT6ILhG9IlUYV74PdsJtOWVIRyBvvpaHdNb/4C+gPSCu9NTdk552eTI7ioqgVjvCLVr10Fw8StkIK+IS4WlL2LBINat1n2qZH+ySc8ktFCktp8rGHUMfn4m97Vl3hjxPJVKIdB5zdHwy0uLh/cKFURy6FeMeJGNZdBKnYpO1znBzZ9gILVngAPlJofggkPWYa4NFQ7dFY+ylY2VQpPBkEz/t4ZCMT+u9AqiRTrjIclDvmE9Z5TkELXJGVSIxPCQe1WqspLndwCPlm2Nd1h9GrXqFElIkFWFLmXPbOpTZ6ZT81rGusasxI3GwyytjnRKpR8WqUhc20aU0ZD1EnSfGMjHDyE5fPef1LXN0DKXCmz8QIsrZKkb9R1DJYGomK7toCfKGUry3VCmtuiNrH116OS+6fUmUyAC73a93Sb/4ZH3RloYCCJnZtOQF+/WqomCyOR5CAzXMeuXIRRKfKfuaek40H+g5h4CsGLuYooesEounOGQTkNk496fmSfZh+pK/ch8w1Mxu450j0HwD+olnVbNeKy0OIHt9HcHo7QjQwr25wE6G+TPpBK3yr9J0nORN5IQI8uDccFxABYeCFTLuAPMA2foH84gOoN0nfSSGt2U5Hl3LH8rwkU05o+1HCIGI14CpWhDYq5gTIcv2jHzIqnnjqDGZg4h0DcNAf6opoCkAEpAwKzWvHUciKTDaC77K3o")
