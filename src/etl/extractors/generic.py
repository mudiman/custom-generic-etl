'''
Created on Oct 20, 2011

@author: Mudassar
'''

from abc import abstractmethod


class ExtractorSkeleton(object):
    
    @abstractmethod
    def get_categories(self, **kwargs):
        raise NotImplementedError()
    
    @abstractmethod
    def get_items(self, **kwargs):
        raise NotImplementedError()
    
    @abstractmethod
    def get_item_detail(self, **kwargs):
        raise NotImplementedError()
    
    @abstractmethod
    def get_item_analytics(self, **kwargs):
        raise NotImplementedError()
