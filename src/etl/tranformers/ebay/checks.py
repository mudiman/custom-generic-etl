'''
Created on Oct 21, 2011

@author: Mudassar
'''
from elementtree.ElementTree import fromstring
from etl.exceptions import TransformerException
from etl.miscfunctions import create_list, remove_namespace
import itertools



      

        

def convert_and_check_xml(api):
    """
        Convert xml into element tree obj and check xml acknowledgement
    """    
    def wrap(f):
        def decorator(**kwargs):
            try:
                xml = kwargs.get('xml', None)
                temp = xml
                xml = fromstring(xml) 
                kwargs['xml'] = xml
                if api in ("FINDINGAPI", "BESTMATCHAPI"):
                    namespace = 'http://www.ebay.com/marketplace/search/v1/services'
                    remove_namespace(xml, namespace)
                    if xml.findtext('ack') == "Failure":
                        print temp
                        raise TransformerException('Ack failure from ebay')
                    if xml.findtext('ack') == "Error":
                        print temp
                        raise TransformerException('Ack Error from ebay')
                elif api == "TRADINGAPI":
                    namespace = 'urn:ebay:apis:eBLBaseComponents'
                    remove_namespace(xml, namespace)
                    if xml.findtext('Ack') == "Failure":
                        raise TransformerException('Ack failure from ebay')
                return f(**kwargs)
            except Exception, e:
                raise TransformerException(str(e))
        return decorator
    return wrap
