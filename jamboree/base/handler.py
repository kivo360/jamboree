from abc import ABC, ABCMeta
from typing import Dict, Any
import copy
from .main import EventProcessor


class BaseHandler(object, metaclass=ABCMeta):
    """ 
        A way to handle reads and writes consistently without having to write every single variable:

         
    """
    def __init__(self):
        pass

    def check(self):
        raise NotImplementedError
    
    def save(self, data:dict):
        raise NotImplementedError
    
    def _get_many(self):
        raise NotImplementedError

    
    def last(self):
        raise NotImplementedError

    def many(self, limit=100):
        raise NotImplementedError
    
    def save_many(self, query, data):
        raise NotImplementedError
    
class DBHandler(BaseHandler):
    """ 
        A way to handle reads and writes consistently without having to write every single variable:

         
    """
    def __init__(self):
        # print("DBHandler")
        self._entity = ""
        self._required = {}
        self._query = {}
        self.data = {}
        self.event_proc = None



    def __setitem__(self, key, value):
        if bool(self.required):
            # print(key)
            # print(self.required)
            # print(value)
            if key in self.required:
                self._query[key] = value
                return self._query
        self.data[key] = value
        return self.data

    def __getitem__(self, key):
        if (not bool(self.data)):
            if key in self.data:
                return self.data[key]
        if self.query is not None:
            if key in self.query:
                return self.query[key]
            return None
        return None


    @property
    def event(self):
        return self.event_proc
    
    @event.setter
    def event(self, _event:EventProcessor):
        # Use to process event
        self.event_proc = _event


    @property
    def entity(self):
        return self._entity

    @entity.setter
    def entity(self, _entity:str):
        self._entity = str(_entity)

    @property
    def required(self):
        return self._required
    
    @required.setter
    def required(self, _required:Dict[str, Any]):
        # check to make sure it's not empty
        self._required = _required


    @property
    def query(self):
        return self._query


    @required.setter
    def query(self, _query:Dict[str, Any]):
        if len(_query.keys()) > 0:
            self._query = _query


    def check(self):
        if self.event_proc is None:
            raise AttributeError("Event processor isn't available.")
        
        if (not bool(self._entity)) or (not bool(self._required)) or (not bool(self._query)):
            raise AttributeError(f"One of the key variables is missing.")
        
        for req in self._required.keys():
            _type = self._required[req]
            if req not in self._query:
                raise AttributeError(f"{req} is not in the requirements")
            if not isinstance(self._query[req], _type):
                raise AttributeError(f"{req} is not a {_type}")
        return True
    
    

    def save(self, data:dict, alt={}):
        self.check()

        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        self.event_proc.save(query, data)
    

    def save_many(self, data:list, alt={}):
        self.check()

        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        self.event_proc._bulk_save(query, data)
    

    def _get_many(self, limit:int, alt={}):
        """ Aims to get many variables """
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        latest_many = self.event_proc.get_latest_many(query, limit=limit)
        return latest_many
    
    def _get_latest(self, alt={}):
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        latest = self.event_proc.get_latest(query)
        return latest


    def last(self, alt={}):
        return self._get_latest(alt)


    def many(self, limit=1000, alt={}):
        return self._get_many(limit, alt=alt)


    def count(self, alt={}):
        """ Aims to get many variables """
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        return self.event_proc.count(query)