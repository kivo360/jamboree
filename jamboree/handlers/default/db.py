
import copy
import inspect
import pprint
from typing import Any, Dict, Optional, AnyStr

import ujson
from loguru import logger

from jamboree import Jamboree, JamboreeNew
from jamboree.base.processors.abstracts import EventProcessor, Processor
from jamboree.handlers.base import BaseHandler
from jamboree.handlers.default.search import BaseSearchHandler
from jamboree.utils import memoized_method
from jamboree.utils.helper import Helpers


class DBHandler(BaseHandler):
    """ 
        A simple event store using a variation of databases.
        ---
        
        Currently uses zadd to work
    """

    def __init__(self):
        # print("DBHandler")
        self._metatype = "event"
        self._entity = ""
        self._required = {}
        self._query = {}
        self.data = {}
        self._is_event = True
        self._processor: Optional[Processor] = None
        self.event_proc: Optional[EventProcessor] = None
        self.main_helper = Helpers()

    def __setitem__(self, key, value):
        if bool(self.required):
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
        return self.setup_key_with_lru(self._query, self.entity, self._metatype, self.data, alt)

    def setup_key_with_lru(self, query:dict, entity:str, metatype:str, data:dict, alt:dict):
        query = copy.copy(query)
        query['type'] = entity
        query['mtype'] = metatype
        query.update(alt)
        query.update(data)
        return query


    @property
    def is_event(self) -> bool:
        """ Determines if we're going to add event ids to what we're doing. We can essentially set certain conditions"""
        return self._is_event
    
    @is_event.setter
    def is_event(self, is_true:bool=False):
        self._is_event = is_true

    @property
    def processor(self) -> Processor:
        if self._processor is None:
            raise AttributeError("The Processor is missing")
        return self._processor
    
    @processor.setter
    def processor(self, _processor: 'Processor'):
        self._processor = _processor


    @property
    def event(self) -> Optional['EventProcessor'] :
        return self.event_proc

    @event.setter
    def event(self, _event: EventProcessor):
        # Use to process event
        self.event_proc = _event
    
    def clear_event(self) -> None:
        self.event_proc = None
        self._processor = None

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

    @query.setter
    def query(self, _query: Dict[str, Any]):
        if len(_query.keys()) > 0:
            self._query = _query

    def check(self):
        
        # self.processor
        # if self.event_proc is None:
        #     raise AttributeError("Event processor isn't available.")
        if (not bool(self._entity)):
            raise AttributeError("Entity hasn't been set")
        
        if (not bool(self._required)):
            raise AttributeError("None of the required information has been set")
        
        if (not bool(self._query)):
            raise AttributeError("None of the queryable information has been set")
        

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
        latest_many = self.processor.event.get_latest_many(query, abs_rel=ar, limit=limit)
        return latest_many

    def _get_latest(self, ar, alt={}):
        query = self.setup_query(alt)
        
        latest = self.processor.event.get_latest(query, abs_rel=ar)
        return latest


    def _last_by(self, time_index:float, ar="absolute", alt={}) -> dict:
        query = self.setup_query(alt)
        return self.processor.event.get_latest_by(query, time_index, abs_rel=ar)


    def _in_between(self, min_epoch:float, max_epoch:float, ar:str="absolute", alt={}):
        query = self.setup_query(alt)
        return self.processor.event.get_between(query, min_epoch, max_epoch, abs_rel=ar)


    def save(self, data: dict, alt={}):
        self.check()
        query = self.setup_query(alt)
        if self.is_event:
            data = self.main_helper.add_event_id(data)
        self.processor.event.save(query, data)

    def save_many(self, data: list, ar="absolute", alt={}):
        self.check()
        if not self.main_helper.is_abs_rel(ar): return
        query = self.setup_query(alt)
        if self.is_event:
            data = self.main_helper.add_event_ids(data)
        self.processor.event.save_many(query, data)


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
        self.processor.event.remove_first(query)


    def pop_many(self, _limit, alt={}):
        self.check()
        query = self.setup_query(alt)
        return self.processor.event.pop_multiple(query, _limit)


    
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
        return self.processor.event.count(query)
    

    def get_single(self, alt={}, is_serialized=True):
        self.check()
        query = self.setup_query(alt)
        item = self.processor.event.single_get(query, is_serialized=is_serialized)
        return item

    def set_single(self, data:dict, alt={}, is_serialized=True):
        self.check()
        query = self.setup_query(alt)
        self.processor.event.single_set(query, data, is_serialized=is_serialized)

    def delete_single(self, alt={}, is_dumps=False):
        self.check()
        query = self.setup_query(alt)
        self.processor.event.single_delete(query)
    
    def delete_all(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        self.processor.event.delete_all(query)

    def query_all(self, alt:Dict[AnyStr, Any]={}):
        self.check()
        query = self.setup_query(alt)
        items = self.processor.event.get_all(query)
        return items

    def get_minimum_time(self, alt:Dict[AnyStr, Any]={}):
        self.check()
        _query = self.setup_query(alt)
        _time = self.processor.event.min_time(_query)
        return _time

    def get_maximum_time(self, alt:Dict[AnyStr, Any]={}):
        self.check()
        _query = self.setup_query(alt)
        _time = self.processor.event.max_time(_query)
        return _time

    def copy(self):
        """ Get everything about this DBHandler without the event inside """
        # _event = self.event
        # self.event = _event
        current_dict = copy.copy(self.__dict__)
        non_lock_types = {

        }
        _process = self.processor
        self._processor = None
        for key, value in current_dict.items():
            classified = type(value)
            if inspect.isclass(classified):
                if isinstance(value, BaseHandler) or issubclass(classified, BaseHandler) or issubclass(classified, BaseSearchHandler):
                    non_lock_types[key] = value
                    setattr(self, key, None)

        

        self.clear_event()
        copied:self = copy.deepcopy(self)
        copied.processor = _process
        for k, v in non_lock_types.items():
            setattr(copied, k, v)

        self.__dict__ = current_dict
        self.processor = _process
        return copied
    
    def lock(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        return self.processor.event.lock(query)
