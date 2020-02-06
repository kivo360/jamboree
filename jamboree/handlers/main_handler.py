import copy
from abc import ABC, ABCMeta
from typing import Dict, Any, List
from loguru import logger
from crayons import red
import ujson
from jamboree.base.processor import EventProcessor
from jamboree.handlers.base import BaseHandler
from jamboree.utils.helper import Helpers



class DBHandler(BaseHandler):
    """ 
        A simple event store using a variation of databases.
        ---
        Currently uses zadd to work
    """

    def __init__(self):
        # print("DBHandler")
        self._entity = ""
        self._required = {}
        self._query = {}
        self.data = {}
        self.event_proc = None
        self.main_helper = Helpers()
        self._is_event = True

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
        if key in self._query.keys():
            return self._query.get(key, None)
        else:
            if key in self.data.keys():
                return self.data.get(key, None)
        return None
    
    def setup_query(self, alt={}):
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return query


    @property
    def is_event(self) -> bool:
        """ Determines if we're going to add event ids to what we're doing. We can essentially set certain conditions"""
        return self._is_event
    
    @is_event.setter
    def is_event(self, is_true:bool=False):
        self._is_event = is_true

    @property
    def event(self):
        return self.event_proc

    @event.setter
    def event(self, _event: EventProcessor):
        # Use to process event
        self.event_proc = _event
    
    def clear_event(self) -> None:
        self.event_proc = None

    @property
    def entity(self):
        return self._entity

    @entity.setter
    def entity(self, _entity: str):
        self._entity = str(_entity)

    @property
    def required(self):
        return self._required

    @required.setter
    def required(self, _required: Dict[str, Any]):
        # check to make sure it's not empty
        self._required = _required

    @property
    def query(self):
        return self._query

    @required.setter
    def query(self, _query: Dict[str, Any]):
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

    

    def _get_many(self, limit: int, ar:str, alt={}):
        """ Aims to get many variables """
        query = self.setup_query(alt)
        latest_many = self.event_proc.get_latest_many(query, abs_rel=ar, limit=limit)
        return latest_many

    def _get_latest(self, ar, alt={}):
        query = self.setup_query(alt)
        latest = self.event_proc.get_latest(query, abs_rel="absolute")
        return latest


    def _last_by(self, time_index:float, ar="absolute", alt={}) -> dict:
        query = self.setup_query(alt)
        return self.event_proc.get_latest_by(query, time_index, abs_rel=ar)


    def _in_between(self, min_epoch:float, max_epoch:float, ar:str="absolute", alt={}):
        query = self.setup_query(alt)
        return self.event_proc.get_between(query, min_epoch, max_epoch, abs_rel=ar)


    def save(self, data: dict, alt={}):
        self.check()
        query = self.setup_query(alt)
        if self.is_event:
            data = self.main_helper.add_event_id(data)
        self.event_proc.save(query, data)

    def save_many(self, data: list, ar="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return
        query = self.setup_query(alt)
        if self.is_event:
            data = self.main_helper.add_event_ids(data)
        self.event_proc.save_many(query, data)


    def last(self, ar="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return {}
        alt.update(self.data)
        return self._get_latest(ar=ar, alt=alt)

    
    def many(self, limit=1000, ar="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return []
        alt.update(self.data)
        return self._get_many(limit, ar, alt=alt)


    def pop(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        self.event_proc.remove_first(query)


    def pop_many(self, _limit, alt={}):
        self.check()
        query = self.setup_query(alt)
        return self.event_proc.pop_multiple(query, _limit)


    
    def last_by(self, time_index:float, ar="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return {}
        item = self._last_by(time_index, ar=ar, alt=alt)
        return item    


    def in_between(self, min_epoch:float, max_epoch:float, ar:str="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return []
        items = self._in_between(min_epoch, max_epoch, ar=ar, alt=alt)
        return items



    def count(self, alt={}) -> int:
        """ Aims to get many variables """
        self.check()
        query = self.setup_query(alt)
        return self.event_proc.count(query)
    

    def get_single(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        item = self.event_proc.single_get(query)
        return item

    def set_single(self, data:dict, alt={}):
        self.check()
        query = self.setup_query(alt)
        self.event_proc.single_set(query, data)

    def delete_single(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        self.event_proc.single_delete(query)
    
    def copy(self):
        """ Get everything about this DBHandler without the event inside """
        _event = self.event
        self.clear_event()
        copied = copy.deepcopy(self)
        self.event = _event
        return copied
    
    # def __str__(self) -> str:
    #     """ 
    #         self._entity = ""
    #         self._required = {}
    #         self._query = {}
    #         self.data = {}
    #         self.event_proc = None
    #         self.main_helper = Helpers()
    #         self._is_event = True
    #     """
    #     total_dict = {
    #         "entity": self._entity,
    #         "required": self._required,
    #         "query": self._query,
    #         "data": self.data,
    #         "is_event": self._is_event
    #     }
    #     rhash = self.main_helper.generate_hash(total_dict)
    #     return rhash