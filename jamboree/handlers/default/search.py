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
from redisearch import Client, Query
from redis.exceptions import ResponseError

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
            
            # self.process_subfields()

    def is_sub(self, name:str) -> bool:
        """ Check to see if this is a subfield """
        return name in self.subnames


    def is_valid_sub_key_information(self, subkey_dict:dict):
        """ Check to see if the subkey is valid"""
        
        if len(subkey_dict) == 0:
            return False
        
        
        # Run validation to see if all of the keys are reducible to a type and base type
        for k, v in subkey_dict.items():
            if is_generic(v):
                continue
            if isinstance(v, dict):
                if not is_queryable_dict(v):
                    logger.error(f"{k} is not valid")
                    return False
        return True


    def reset_builders(self):
        self.insert_builder = InsertBuilder()
        self.query_builder = QueryBuilder()
    
        


class BaseSearchHandler(BaseSearchHandlerSupport):
    def __init__(self):
        super().__init__()
        self._entity = None
        # This will only be here as an example
        self.is_replacement = False
        self.current_replacement = None
        self.is_set_entity = False
                
        # Subs are all of the subfields we would need to search through
        self.subs:Dict[str, BaseSearchHandler] = {}

        self.current_doc_id = None
        self.current_doc_id_list = None
        self.current_client = None
    
    def __setitem__(self, key:str, value:Any):
        if key not in self.requirements.keys() and (not self.is_replacement):
            return
        self.is_set_entity = False
        self.current_client = None
        if isinstance(value, dict):
            self.handle_input_dict_key(key, value) 
        else:
            _instance_type = type(value)
            # check that the value is the right type
            
            logger.debug(f"Skipping: {key} - {value}")
            if is_generic(_instance_type):
                _str_type = to_str(_instance_type)
                self.query_builder.insert_by_type_str(_str_type, key, value)
                self.insert_builder.insert_by_type_str(_str_type, key, value)


    @property
    def replacement(self):
        if self.current_replacement is None:
            self.current_replacement =  BaseSearchHandler()
            self.current_replacement.is_replacement = True
            self.current_replacement.insert_builder.is_replacement = True
        return self.current_replacement

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
        _requirements['document_id'] = str
        self.process_requirements(_requirements)
        if not self.is_sub_key:
            self.create_sub_handlers()
    @property
    def doc_id(self):
        return self.current_doc_id

    @doc_id.setter
    def doc_id(self, _doc_id:str):
        self.current_doc_id = _doc_id


    @property
    def verbatim(self):
        return self.query_builder.build_exact()

    @property
    def client(self):
        if self.current_client is None:
            try:
                self.current_client = Client(self.index)
                self.current_client.create_index(self.indexable)
            except ResponseError:
                pass
        return self.current_client


    def create_sub_handlers(self):
        """ Creates subhandlers for the given index"""
        for name, subkey in self.subfields.items():
            subhandler = BaseSearchHandler()
            subhandler.is_sub_key = True
            subhandler.index = subkey
            self.subs[name] = subhandler

    def set_entity(self):
        if self.is_set_entity is False:
            self['entity'] = {
                "type": "TEXT",
                "is_filter": True,
                "values": {
                    "is_exact": True,
                    "term": self.entity
                }
            }
            self.is_set_entity = True


    def verbatim_docs(self):
        results = self.client.search(self.verbatim)
        result_docs = results.docs
        return result_docs

    def handle_input_dict_key(self, name:str, item:dict):
        """ Figures out where to put the input dictionary for the query """
        if self.is_sub(name):
            # If this is a subkey we'll run the same operation again
            # Check to see if the subkey is empty and has information that is reducible to "type"
            pass
        else:
            # If it's not queryable don't try adding anything
            if not is_queryable_dict(item):
                return

            self.insert_builder.from_dict(name, item)
            self.query_builder.from_dict(name, item)


    def find(self, alt={}):
        """Given the items we've set, find all matching items"""
        
        self.set_entity()
        qstring = self.query_builder.build()
        results = self.client.search(qstring)
        result_docs = results.docs
        return result_docs
        
        
    
    def update(self, alt={}):
        """
            # UPDATE

            Given the items or ID we've set, partial update every matching document. 
            If we have the document_ids already, replace those items
        """
        self.set_entity()
        result_docs = self.verbatim_docs()
        replacement_variables = self.replacement.insert_builder.build()
        batcher = self.client.batch_indexer(chunk_size=len(result_docs))
        for result in result_docs:
            doc_id = result.id
            batcher.add_document(doc_id, replace=True, partial=True, **replacement_variables)
        batcher.commit()
    
    def insert(self, alt={}, allow_duplicates=False):
        """
            # INSERT

            Given all of the items we've set, add documents
        """
        self.set_entity()
        
        verbatim_docs = self.verbatim_docs()
        if len(verbatim_docs) > 0 and allow_duplicates == False:
            # Not adding docs because we're not allowing duplicates
            return
        insert_variables = self.insert_builder.build()
        _doc_id = self.insert_builder.doc_id

        self.client.add_document(_doc_id, payload=_doc_id, **insert_variables)

        
    
    def remove(self, alt={}):
        """
            # REMOVE

            Remove all documents that match a given ID
        """
        self.set_entity()
        results = self.client.search(self.verbatim)
        result_docs = results.docs
        for result in result_docs:
            doc_id = result.id
            self.client.delete_document(doc_id)
    
    def reset(self):
        """Reset all local variables"""
        self.reset_builders()
        self.is_set_entity = True
        self.is_replacement = False
        self.current_replacement = None
        self.current_client = None

class ExampleSearchHandler(BaseSearchHandler):
    def __init__(self):
        super().__init__()
        self.entity = "example"
        self.requirements = {
            "name": str,
            "rano": float,
            "category": str,
            "sample_tags": list,
            "loc": "GEO",
            "live": bool
        }


def main():
    example_handler = ExampleSearchHandler()
    example_handler['name'] = "Kevin Hill"
    example_handler['category'] = "markets"
    example_handler['sample_tags'] = ["one", "two", "three"]
    example_handler['rano'] = {
        "type": "NUMERIC",
        "is_filter": True,
        "values": {
            "lower": -1,
            "upper": 34,
            "operation": "between"
        }
    }
    example_handler['live'] = False
    example_handler['loc'] = {
        "type": "GEO",
        "is_filter": True,
        "values": {
            "long": 33.4,
            "lat": 2.5,
            "distance": 8000,
            "metric": "km"
        }
    }
    example_handler.replacement['live'] = True
    example_handler.insert()
    records = example_handler.find()
    logger.warning((records, len(records)))

    example_handler.insert()
    records = example_handler.find()
    logger.debug((records, len(records)))

    example_handler.insert(allow_duplicates=True)
    records = example_handler.find()
    logger.error((records, len(records)))



if __name__ == "__main__":
    main()
