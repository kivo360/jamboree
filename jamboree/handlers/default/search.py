import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from typing import Dict, Any
import time
from jamboree.utils.core import consistent_hash
from jamboree.utils.support.search import ( QueryBuilder, InsertBuilder,
                                            is_gen_type, is_generic, is_geo,
                                            is_nested, name_match, to_field,
                                            to_str, is_queryable_dict)
from loguru import logger
from cerberus import Validator
from redisearch import Client, Query
from pprint import pprint
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
            self.process_subfields()

    def is_sub(self, name:str) -> bool:
        """ Check to see if this is a subfield """
        return name in self.subnames

    def is_queryable(self, _dict):
        if isinstance(_dict, dict):
            if is_queryable_dict(_dict):
                return True
        return False

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

    def queryable_to_type(self, _dict:dict):
        """ Converts a queryable dictionary into a type"""
        dtype = _dict['type']
        if dtype == "GEO":
            return "GEO"
        elif dtype == "TEXT":
            return str
        elif dtype == "BOOL":
            return bool
        elif dtype == "NUMERIC":
            return float        
        elif dtype == "TAG":
            return list

    def loaded_dict_to_requirements(self, _dict:dict):
        """ 
            # Loaded Dict To Requirements
            
            Convert a dictionary into a requirements dict. 

            Use to create a requirements

            Returns an empty dict if nothing is there.
        """
        req = {}
        for k, v in _dict.items():
            _ktype = type(v)
            if is_generic(_ktype):
                req[k] = _ktype
            if self.is_queryable(v):
                req[k] = self.queryable_to_type(v)
                
        return req


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
        self.use_sub_query = False
        self.print_sub = False
    
    def __setitem__(self, key:str, value:Any):
        if key not in self.requirements.keys() and (not self.is_replacement):
            return
        self.is_set_entity = False
        self.current_client = None
        if isinstance(value, dict):
            if len(value) == 0:
                return
            self.use_sub_query = True
            self.handle_input_dict_key(key, value) 
        else:
            _instance_type = type(value)
            # check that the value is the right type
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
        
        if self.is_sub_key:
            for i in self.indexable:
                try:
                    self.current_client.alter_schema_add([i])
                except ResponseError as res:
                    pass
        return self.current_client

    
    @property
    def subinserts(self):
        """
            # SUBINSERT Dicts
            
            Gets the insert dictionaries for the given
        """
        pass


    def create_sub_handlers(self):
        """ Creates subhandlers for the given index"""
        for name, subkey in self.subfields.items():
            subhandler = BaseSearchHandler()
            subhandler.is_sub_key = True
            subhandler.index = subkey
            subhandler.insert_builder.is_sub = True
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

    # def verbatim_super_id(self):
    #     q = Query(self.query_builder.build()).paging(0, 1000000)
    #     results = self.client.search(q)
    #     result_docs = results.docs
    #     return result_docs

    def verbatim_docs(self):
        q = Query(self.query_builder.build()).paging(0, 1000000)
        results = self.client.search(q)
        result_docs = results.docs
        return result_docs
    
    def verbatim_sub_ids(self):
        id_set = set()
        for sub in self.subs.values():
            sub.print_sub = True
            verb_items = sub.verbatim_docs()
            for verb in verb_items:
                try:
                    id_set.add(verb.super_id)
                except Exception:
                    pass
        return list(id_set)

    def handle_input_dict_key(self, name:str, item:dict):
        """ Figures out where to put the input dictionary for the query """
        if self.is_sub(name):
            # If this is a subkey we'll run the same operation again
            # Check to see if the subkey is empty and has information that is reducible to "type"
            reqs = self.loaded_dict_to_requirements(item)
            self.subs[name].requirements = reqs
            for k, v in item.items():
                self.subs[name][k] = v
        else:
            # If it's not queryable don't try adding anything
            if not is_queryable_dict(item):
                return

            self.insert_builder.from_dict(name, item)
            self.query_builder.from_dict(name, item)


    def _normal_find(self, limit_ids=None):
        # logger.success(len(limit_ids))
        q = Query(self.query_builder.build()).paging(0, 1000000)
        if limit_ids is not None and len(limit_ids) > 0:
            q.limit_ids(*limit_ids)
        results = self.client.search(q)
        result_docs = results.docs
        return result_docs
    
    def _sub_find(self):
        sub_ids = self.verbatim_sub_ids()
        if len(sub_ids) == 0:
            return []
        return self._normal_find(limit_ids=sub_ids)

    def find(self, alt={}):
        """Given the items we've set, find all matching items"""
        
        self.set_entity()
        return self._sub_find()
        
        # return result_docs
        
        
    
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
    
    def _normal_insert(self, allow_duplicates=False):
        if allow_duplicates == False:
            verbatim_docs = self.verbatim_docs()
            if len(verbatim_docs) > 0 and allow_duplicates == False:
                # Not adding docs because we're not allowing duplicates
                return "", False
        insert_variables = self.insert_builder.build()
        _doc_id = self.insert_builder.doc_id
        self.client.add_document(_doc_id, payload=_doc_id, **insert_variables)
        return _doc_id, True

    def _sub_insert(self, allow_duplicates=False):
        _super_id, _did_insert = self._normal_insert(allow_duplicates=allow_duplicates)
        if _did_insert:
            for sub in self.subs.values():
                sub.insert_builder.super_id = _super_id
                sub._normal_insert(allow_duplicates=True)

    def insert(self, alt={}, allow_duplicates=False):
        """
            # INSERT

            Given all of the items we've set, add documents
        """
        self.set_entity()
        
        self._sub_insert(allow_duplicates=allow_duplicates)

        
    
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
            "category": str,
            "subcategories": dict,
            "live": bool,
            "loc": "GEO"
        }


def main():
    example_handler = ExampleSearchHandler()
    example_handler['name'] = "Boi Gurl"
    example_handler['category'] = "markets"
    example_handler['sample_tags'] = ["one", "two", "three"]
    example_handler['subcategories'] = {
        "my": 123,
        "country": "US",
        "lurk": {
            "type": "GEO",
            "is_filter": True,
            "values": {
                "long": 35,
                "lat": 5.4,
                "distance": 80,
                "metric": "km"
            }
        }
    }
    example_handler['live'] = False
    example_handler.replacement['live'] = True
    start = time.time()
    for _ in range(10000):
        example_handler.insert(allow_duplicates=True)
    # example_handler.find()
    logger.debug(time.time()-start)
    # logger.warning((records, len(records)))

    # example_handler.insert()
    # records = example_handler.find()
    # logger.debug((records, len(records)))

    # example_handler.insert(allow_duplicates=True)
    # records = example_handler.find()
    # logger.error((records, len(records)))



if __name__ == "__main__":
    main()
