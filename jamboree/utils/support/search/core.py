import os

import time
import warnings
from copy import copy
from pprint import pprint
from typing import Any, Dict, List
warnings.simplefilter(action='ignore', category=FutureWarning)


from addict import Dict as ADict
from cerberus import Validator
from eliot import log_call, to_file
from loguru import logger
from redis.exceptions import ResponseError
from redisearch import Client, Query

from jamboree.utils.core import consistent_hash
from jamboree.utils.support.search import (InsertBuilder, QueryBuilder,
                                           is_gen_type, is_generic, is_geo,
                                           is_nested, is_queryable_dict,
                                           name_match, to_field, to_str)
from jamboree.utils.support.search.assistance import Keystore


logger.disable(__name__)
"""

    # NOTE

    Basic CRUD operations for the search handler. 
"""

REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_HOST = str(os.getenv("REDIS_HOST", "localhost"))

def split_doc(doc):
    return doc.id, ADict(**doc.__dict__)

def dictify(doc):
    item = ADict(**doc.__dict__)
    item.pop("super_id", None)
    item.pop("payload", None)
    return item

class BaseSearchHandlerSupport(object):
    def __init__(self):
        self._requirements_str = {
            
        }
        self._subkey_names = set()
        self._indexable = set()
        self.__indexable = []
        self._index_key:str = ""
        self._sub_fields = {}
        self.insert_builder = InsertBuilder()
        self.query_builder = QueryBuilder()
        self.keystore = Keystore()
        self.added = set()
        # Boolean explaining if this is a subquery
        self.is_sub_key = False
    
    
    @property
    def indexable(self):
        return self.__indexable
    
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
                _agg = f"{k}:{sval}"
                if _agg not in self.added:
                    self.added.add(_agg)
                    self._requirements_str[k] = sval
                    field = to_field(k, sval)
                    
                    self.__indexable.append(field)
                continue
                
            if v == dict:
                _agg = f"{k}:SUB"
                if _agg not in self.added:
                    self.added.add(_agg)
                    self._requirements_str[k] = "SUB"
                    self.subnames.add(k)
                continue

            if is_geo(v):
                _agg = f"{k}:GEO"
                if _agg not in self.added:
                    self.added.add(_agg)
                    self._requirements_str[k] = "GEO"
                    self.__indexable.append(to_field(k, "GEO"))

                continue
        
        # self._indexable = set(unique(self._indexable, key=lambda x: x.redis_args()[0]))
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