"""
    A class that holds all of the helper functions.
"""
import base64
from copy import copy
from abc import ABC

import maya
import ujson
import orjson


class Helpers(object):
    def __init__(self) -> None:
        pass

    def generate_hash(self, query:dict):
        _hash = ujson.dumps(query, sort_keys=True)
        _hash = base64.b64encode(str.encode(_hash))
        _hash = _hash.decode('utf-8')
        return _hash
    
    def validate_query(self, query:dict):
        """ Validates a query. Must have `type` and a second identifier at least"""
        if 'type' not in query:
            return False
        if not isinstance(query['type'], str):
            return False
        if len(query) < 2:
            return False
        return True

    def update_dict(self, query:dict, data:dict):
        query = copy(query)
        timestamp = maya.now()._epoch
        query['timestamp'] = timestamp
        data.update(query)
        return data

    def update_dict_no_timestamp(self, query:dict, data:dict):
        query = copy(query)
        data = copy(data)
        data.update(query)
        data.pop("timestamp", None)
        return data

    def back_to_dict(self, list_of_serialized:list):
        deserialized = []
        if len(list_of_serialized) == 1:
            return orjson.loads(list_of_serialized[0])
        
        for i in list_of_serialized:
            
            deserialized.append(orjson.loads(i))
        return deserialized
    
    def search_one(self, item:dict, query:dict):
        all_bools = []
        for q in query:
            if q in item:
                if query[q] == query[q]:
                    all_bools.append(True)
                else:
                    all_bools.append(False)
            else:
                all_bools.append(False)
        return any(all_bools)