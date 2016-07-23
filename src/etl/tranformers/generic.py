'''
Created on Oct 21, 2011

@author: Mudassar
'''

class TransformersSkeleton(object):
    
    def transform_get_categories(self):
        raise NotImplementedError()
    
    def transform_get_items(self):
        raise NotImplementedError()
    
    def transform_get_item_detail(self):
        raise NotImplementedError()
    
    def transform_get_item_analytics(self):
        raise NotImplementedError()
