from abc import ABC, ABCMeta
import copy

from typing import Dict, Any, List
from loguru import logger
from .processor import EventProcessor


class BaseHandler(object, metaclass=ABCMeta):
    """ 
        A way to handle reads and writes consistently without having to write every single variable:
    """

    def __init__(self):
        pass

    def check(self):
        raise NotImplementedError

    def save(self, data: dict):
        raise NotImplementedError

    def _bulk_save(self, query: dict, data: list):
        raise NotImplementedError

    def _get_many(self):
        raise NotImplementedError

    def last(self):
        raise NotImplementedError

    def many(self, limit: int = 100):
        raise NotImplementedError

    def save_many(self, query: dict, data: list):
        raise NotImplementedError

    def pop_multiple(self, query, _limit: int = 1):
        raise NotImplementedError

    def swap(self, query, alt: dict = {}):
        """ Swap betwen the first and last item """
        raise NotImplementedError

    def query_mix(self, query: dict, alt: dict = {}):
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

    @property
    def event(self):
        return self.event_proc

    @event.setter
    def event(self, _event: EventProcessor):
        # Use to process event
        self.event_proc = _event

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

    def save(self, data: dict, alt={}):
        self.check()

        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        self.event_proc.save(query, data)

    def save_many(self, data: list, alt={}):
        self.check()

        query = copy.copy(self._query)
        query['type'] = self.entity
        # logger.info(query)
        query.update(alt)
        query.update(self.data)
        self.event_proc._bulk_save(query, data)

    def _get_many(self, limit: int, alt={}):
        """ Aims to get many variables """
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        latest_many = self.event_proc.get_latest_many(query, limit=limit)
        return latest_many

    def _get_latest(self, alt={}):
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        latest = self.event_proc.get_latest(query)
        return latest

    def last(self, alt={}):
        alt.update(self.data)
        return self._get_latest(alt)

    def many(self, limit=1000, alt={}):
        alt.update(self.data)
        return self._get_many(limit, alt=alt)

    def pop(self, alt={}):
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        self.event_proc.remove_first(query)

    def pop_many(self, _limit, alt={}):
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return self.event_proc.pop_multiple(query, _limit)

    def count(self, alt={}):
        """ Aims to get many variables """
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return self.event_proc.count(query)

    def swap_many(self, limit: int = 10, alt={}):
        """ Move items from the main list to a swapped list. """
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return self.event_proc.multi_swap(query, limit)

    def query_mix(self, limit: int = 10, alt: dict = {}):
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return self.event_proc.query_mix(query, limit)

    def query_many_swap(self, limit: int = 10, alt: dict = {}):
        self.check()
        query = copy.copy(self._query)
        query['type'] = self.entity
        query.update(alt)
        query.update(self.data)
        return self.event_proc.get_latest_many_swap(query, limit)
