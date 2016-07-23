from etl.tranformers.generic import TransformersSkeleton




class Transformers(TransformersSkeleton):
    
    def transform_get_categories(self):
        raise NotImplementedError()
    
    def transform_get_items(self):
        raise NotImplementedError()
    
    def transform_get_item_detail(self):
        raise NotImplementedError()
    
    def transform_get_item_analytics(self):
        raise NotImplementedError()
