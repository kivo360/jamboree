"""
    A class that holds all of the helper functions.
"""
import base64
from copy import copy
from abc import ABC

import maya
import ujson
import orjson
from typing import List, Set

class Helpers(object):
    def __init__(self) -> None:
        pass

    def generate_hash(self, query:dict):
        _hash = ujson.dumps(query, sort_keys=True)
        _hash = base64.b64encode(str.encode(_hash))
        _hash = _hash.decode('utf-8')
        return _hash
    
    def add_time(self, item:dict, _time:float, rel_abs="absolute"):
        if rel_abs == "absolute":
            item['timestamp'] = _time
        else:
            item['time'] = _time
        return item
    
    def generate_dicts(self, data:dict, _time:float, timestamp:float):
        relative = copy(data)
        absolute = copy(data)
        relative['time'] = _time
        absolute['timestamp'] = _time
        return {
            "relative": relative,
            "absolute": absolute
        }

    def dictify(self, azset:List[Set], rzset:List[Set]):
        """Create a dictionary"""
        if len(azset) == 0 or len(rzset) == 0:
            return {}
        adict = {}
        for azs in azset:
            item, time = azs
            current_item = adict.get(item, {})
            current_item['timestamp'] = time
            adict[item] = current_item
            
        for rzs in rzset:
            item, time = rzs
            current_item = adict.get(item, {})
            current_item['time'] = time
            adict[item] = current_item
        
        return adict
    
    def check_time(self, _time:float=None, _timestamp:float=None, local_time:float=None, local_timestamp:float=None):
        current_time = maya.now()._epoch
        
        if local_time is not None:
            _time = local_time
        elif _time is None:
            _time = current_time
        if local_timestamp is not None:
            _timestamp = local_timestamp
        elif _timestamp is None:
            _timestamp = current_time
        
        return {
            "time": _time,
            "timestamp": _timestamp
        }

    def deserialize_dicts(self, dictified:dict):
        _deserialized = []
        for key, value in dictified.items():
            _key = orjson.loads(key)
            _key['time'] = value.get("time", maya.now()._epoch)
            _key['timestamp'] = value.get("timestamp", maya.now()._epoch)
            _deserialized.append(_key)
        return _deserialized

    def separate_time_data(self, data:dict, _time:float=None, _timestamp:float=None):
        local_time = data.pop("time", None)
        local_timestamp = data.pop("timestamp", None)
        timing = self.check_time(_time, _timestamp, local_time, local_timestamp)
        return data, timing

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