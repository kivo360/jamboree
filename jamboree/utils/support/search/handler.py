import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from typing import Dict, Any

from jamboree.utils.core import consistent_hash
from jamboree.utils.support.search import ( QueryBuilder, InsertBuilder,
                                            is_gen_type, is_generic, is_geo,
                                            is_nested, name_match, to_field,
                                            to_str, is_queryable_dict)
from loguru import logger
from cerberus import Validator


class BaseSearchHandlerSupport(object):
    def __init__(self):
        self._requirements_str = {
            
        }
        self._subkey_names = set()
        self._indexable = set()
        self._index_key:str = ""
        self._sub_fields = {}
        self.insert_builder = InsertBuilder()
        self.query_builder = QueryBuilder()
        
        # Boolean explaining if this is a subquery
        self.is_sub_key = False
    
    @property
    def indexable(self):
        return list(self._indexable)
    
    @property
    def subnames(self):
        return self._subkey_names
    @property
    def index(self):
        """Index key for the requirements"""
        return self._index_key
    
    @index.setter
    def index(self, _index):
        """Index key for the requirements"""
        self._index_key = _index
    
    @property
    def subfields(self):
        return self._sub_fields
    
    def process_subfields(self):
        for key in self.subnames:
            self._sub_fields[key] = f"{self.index}:{key}"
    
    def process_requirements(self, _requirements:dict):
        """
            Process the required fields. That includes:
            
            1. Creating a requirements string. That's so we can create a key representing the field that exist.
            2. Listing all of the subkeys that we'd need to take in consideration.
            3. Creating an index hash to locate all relavent documents
            4. Creation of a list of fields so we can create a schema at that index hash
            5. Creation of all subkeys so we can quickly access them by name later
            
        """
        for k, v in _requirements.items():
            if is_generic(v):
                sval = to_str(v)
                self._requirements_str[k] = sval
                field = to_field(k, sval)
                self._indexable.add(field)
                continue
                
            if v == dict:
                self._requirements_str[k] = "SUB"
                self.subnames.add(k)
                continue

            if is_geo(v):
                self._requirements_str[k] = "GEO"
                self._indexable.add(to_field(k, "GEO"))
                continue
        if not self.is_sub_key:
            self._index_key = consistent_hash(self._requirements_str)
            self.process_subfields()

    def is_sub(self, name:str) -> bool:
        """ Check to see if this is a subfield """
        return name in self.subnames


    def is_valid_sub_key_information(self, subkey_dict:dict):
        """ Check to see if the subkey is valid"""
        
        if len(subkey_dict) == 0:
            return False
        


    
            # We'd define a query here
            # Also define what we're adding



class BaseSearchHandler(BaseSearchHandlerSupport):
    def __init__(self):
        super().__init__()
        self._entity = None
        # This will only be here as an example
        
        
        
        
        # Subs are all of the subfields we would need to search through
        self.subs:Dict[str, BaseSearchHandler] = {}
        # Replacement is a set of fields we'd place in place of the ones we query or find by id
        self.replacement = {}
    
    def __setitem__(self, key:str, value:Any):
        if key not in self.requirements.keys():
            return

        if isinstance(value, dict):
            self.handle_input_dict_key(key, value) 
        else:
            _instance_type = type(value)
            # check tha the value is the right type
            if is_generic(_instance_type):
                _str_type = to_str(_instance_type)
                self.query_builder.insert_by_type_str(_str_type, key, value)
            pass
    


    @property
    def entity(self):
        if self._entity is None:
            raise AttributeError("You haven't set entity yet")
        return self._entity
    
    @entity.setter
    def entity(self, _entity:str):
        self._entity = _entity
    
    @property
    def requirements(self):
        return self._requirements_str
    
    @requirements.setter
    def requirements(self, _requirements:dict):
        """If we set it here we'd go through each dict item and create string version of each key"""
        # Document id will allow us to figure out which documents are involved with subkeys
        _requirements['entity'] = str
        _requirements['doc_id'] = str
        self.process_requirements(_requirements)
        self.create_sub_handlers()
    

    def create_sub_handlers(self):
        """ Creates subhandlers for the given index"""
        for name, subkey in self.subfields.items():
            subhandler = BaseSearchHandler()
            subhandler.is_sub_key = True
            subhandler.index = subkey
            self.subs[name] = subhandler


    def handle_input_dict_key(self, name:str, item:dict):
        """ Figures out where to put the input dictionary for the query """
        if self.is_sub(name) and (not self.is_sub_key):
            # If this is a subkey we'll run the same operation again
            # Check to see if the subkey is empty and has information that is reducible to "type"
            logger.success("We're able to create a new subkey. We don't allow for more than two layers")
        else:
            # If it's not queryable don't try adding anything
            if not is_queryable_dict(item):
                return
            # logger.debug("It's an item we can both query and take values from to insert into the database")
            self.insert_builder.from_dict(name, item)
            self.query_builder.from_dict(name, item)


    def find(self, alt={}):
        """Given the items we've set, find all matching items"""
        self.query_builder.build()
    
    def update(self, alt={}):
        """
            # Update
            Given the items or ID we've set, partial update every matching document. 
            If we have the document_ids already, replace those items
        """
        pass
    
    def insert(self, alt={}):
        """
            # Insert
            Given all of the items we've set, add documents
        """
        pass
    
    def remove(self, alt={}):
        pass
    
    def reset(self):
        """Reset all local variables"""
        pass

class ExampleSearchHandler(BaseSearchHandler):
    def __init__(self):
        super().__init__()
        self.entity = "example"
        self.requirements = {
            "name": str,
            "category": str,
            "subcategories": dict,
            "loc": "GEO",
            "live": bool
        }


def main():
    base_handler = ExampleSearchHandler()
    base_handler['name'] = "Kevin Hill"
    base_handler['category'] = "markets"
    base_handler['subcategories'] = {
        "country": "US"
    }
    base_handler['live'] = False
    base_handler['loc'] = {
        "type": "GEO",
        "is_filter": True,
        "values": {
            "long": 33,
            "lat": -10,
            "distance": 1.2,
            "metric": "km"
        }
    }
    base_handler.find()


if __name__ == "__main__":
    main()
