import copy
from abc import ABC, ABCMeta
from typing import Dict, Any, List


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


class BaseFileHandler(object, metaclass=ABCMeta):
    """ 
        A way to handle reads and writes consistently without having to write every single variable:
    """

    def __init__(self):
        pass

    def check(self):
        raise NotImplementedError

    def save(self, data: dict):
        raise NotImplementedError
    
    def save_version(self, query:dict, data):
        pass

    def last(self):
        raise NotImplementedError

    def many(self, limit: int = 100):
        raise NotImplementedError

    def save_many(self, query: dict, data: list):
        raise NotImplementedError

    def delete(self, query:dict):
        raise NotImplementedError
    
    def delete_version(self, query:dict, version:str):
        raise NotImplementedError