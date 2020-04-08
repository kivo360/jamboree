from jamboree.handlers.default.search import BaseSearchHandler


class MetadataSearchHandler(BaseSearchHandler):
    """
        # 10 metatypes

        Metatypes are
        
        1. Strategy
        2. Data
        3. Model
        4. Feature

    """
    def __init__(self):
        super().__init__()
        self.entity = "metadata"
        self.dreq = {
            "name": str,
            # The type of metadata we're positioning
            # strategy, data, model, metainfo (metadata about metadata) are the clear items in mind
            "metatype": str,
            # another identifiable metatype to narrow down results
            # pricing(data), economic, batch (models), online (models), micro (strategies), macro (strategies), supporting_group
            "submetatype": str,
            "category": str,
            "subcategories": dict,
            "description": str,
            "info": dict,
            "location": "GEO",
        }
    
    