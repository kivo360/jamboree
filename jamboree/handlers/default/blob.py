"""
    Basic storage handler
    ---

"""

import copy
from typing import Any, Dict, Optional

import ujson
from addict import Dict as ADict

from jamboree import JamboreeNew
from jamboree.base.processors.abstracts import EventProcessor, Processor
from jamboree.handlers.base import BaseFileHandler, BaseHandler
from jamboree.utils.helper import Helpers

class BlobStorageHandler(BaseHandler):
    """ 
        A simple event store using a variation of databases.
        ---
        
        Currently uses zadd to work
    """

    def __init__(self):
        # print("DBHandler")
        self._entity = ""
        self._meta_type = "storage"
        self._required = {}
        self._query = {}
        self._data = {}
        self._is_event = True
        self._processor: Optional[Processor] = None
        self.event_proc: Optional[EventProcessor] = None
        self.main_helper = Helpers()
        self.changed_since_command = False

    def __setitem__(self, key, value):
        if bool(self.required):
            
            if key in self.required:
                self._query[key] = value
                return self._query
        self._data[key] = value
        self.changed_since_command = True
        return self._data

    def __getitem__(self, key):
        if key in self._query.keys():
            return self._query.get(key, None)
        else:
            if key in self._data.keys():
                return self._data.get(key, None)
        return None
    
    def setup_query(self, alt={}):
        query = copy.copy(self._query)
        query['type'] = self.entity
        query['mtype'] = self._meta_type
        query.update(alt)
        query.update(self._data)
        return query


    @property
    def is_event(self) -> bool:
        """ Determines if we're going to add event ids to what we're doing. We can essentially set certain conditions"""
        return self._is_event
    
    @is_event.setter
    def is_event(self, is_true:bool=False):
        self._is_event = is_true

    @property
    def processor(self) -> 'Processor':
        if self._processor is None:
            raise AttributeError("The Processor is missing")
        return self._processor
    
    @processor.setter
    def processor(self, _processor: 'Processor'):
        self._processor = _processor

    
    def clear_event(self) -> None:
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

        if (not bool(self._entity)) or (not bool(self._required)) or (not bool(self._query)):
            raise AttributeError(f"One of the key variables is missing.")

        for req in self._required.keys():
            _type = self._required[req]
            if req not in self._query:
                raise AttributeError(f"{req} is not in the requirements")
            if not isinstance(self._query[req], _type):
                raise AttributeError(f"{req} is not a {_type}")
        return True
    

    def save(self, data: dict, alt={}, is_overwrite=False):
        self.check()
        query = self.setup_query(alt)
        # Put settings here
        current_settings = ADict()
        current_settings.overwrite = is_overwrite
        self.processor.storage.save(query, data, **current_settings.to_dict())
        self.changed_since_command = False
    

    def save_version(self, data: dict, version:str, alt={}, is_overwrite=False):
        self.check()
        query = self.setup_query(alt)
        # Put settings here
        current_settings = ADict()
        self.processor.storage.save(query, data, **current_settings.to_dict())
        
        self.changed_since_command = False
    
    def absolute_exists(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        # Put settings here
        current_settings = ADict()
        current_settings.is_force = self.changed_since_command
        avs = self.processor.storage.absolute_exists(query, **current_settings.to_dict())
        self.changed_since_command = False
        return avs

    def last(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        current_settings = ADict()
        self.changed_since_command = False
        obj = self.processor.storage.query(query, **current_settings.to_dict())
        return obj
    
    def by_version(self, version:str, alt={}):
        """ Get the data by version. """
        self.check()
        query = self.setup_query(alt)
        current_settings = ADict()
        self.processor.storage.query(query, **current_settings.to_dict())
        self.changed_since_command = False
    
    def delete(self, query:dict, alt={}):
        self.check()
        query = self.setup_query(alt)
        current_settings = ADict()

        self.processor.storage.delete(query, **current_settings)
        self.changed_since_command = False
    
    def lock(self, alt={}):
        self.check()
        query = self.setup_query(alt)
        self.changed_since_command = False
        return self.processor.event.lock(query)
    
    def clear(self):
        """ Clear in-memory cache. To use with existence checks and rockdb """
        self.changed_since_command = True
