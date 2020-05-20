from jamboree.handlers.abstracted.search import ParameterizedSearch
from jamboree.handlers.default.search import BaseSearchHandler


class MetadataSearchHandler(ParameterizedSearch):
    """
        # 10 metatypes

        Metatypes are
        
        1. Strategy
        2. Data
        3. Model
        4. Meta

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
            # pricing(data), economic, weather (data), social(data), political (data), features (data)
            # batch (models), online (models), micro (strategies), macro (strategies), supporting_group (complex)
            "submetatype": str,
            "category": str,
            "subcategories": dict,
            "description": str,
            "info": dict,
            # The location about the information involved
            "location": "GEO",
            "abbreviation": str
        }
        self.must_have = ['name', 'metatype', 'category', 'submetatype', 'abbreviation']
    
    