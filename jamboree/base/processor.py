from abc import ABC
from typing import List

class EventProcessor(ABC):
    def save(self, query: dict, data: dict, abs_rel="absolute"):
        raise NotImplementedError


    def save_many(self, query: dict, data: List[dict], abs_rel="absolute"):
        raise NotImplementedError    


    def get_latest(self, query, abs_rel="absolute") -> dict:
        raise NotImplementedError


    def get_latest_many(self, query, abs_rel="absolute", limit=1000):
        raise NotImplementedError


    def get_between(self, query:dict, min_epoch:float, max_epoch:float, abs_rel:str="absolute") -> list:
        raise NotImplementedError


    def get_latest_by(self, query:dict, max_epoch, abs_rel="absolute", limit:int=10) -> dict:
        raise NotImplementedError


    def count(self, query: dict) -> int:
        raise NotImplementedError


    def remove_first(self, query: dict):
        raise NotImplementedError

    
    def pop_multiple(self, query: dict, limit: int):
        raise NotImplementedError


    def _bulk_save(self, query: dict, data: list):
        raise NotImplementedError


    def single_get(self, query:dict):
        raise NotImplementedError


    def single_set(self, query:dict, data:dict):
        raise NotImplementedError


    def single_delete(self, query:dict):
        raise NotImplementedError

    

class Processor(ABC):
    """ Use to allow for multiple items"""
    def __init__(self):
        self._data = None
        self._storage = None
        self._search = None

    def save(self, query: dict, data: dict, abs_rel="absolute"):
        raise NotImplementedError


    def save_many(self, query: dict, data: List[dict], abs_rel="absolute"):
        raise NotImplementedError    


    def get_latest(self, query, abs_rel="absolute") -> dict:
        raise NotImplementedError


    def get_latest_many(self, query, abs_rel="absolute", limit=1000):
        raise NotImplementedError


    def get_between(self, query:dict, min_epoch:float, max_epoch:float, abs_rel:str="absolute") -> list:
        raise NotImplementedError


    def get_latest_by(self, query:dict, max_epoch, abs_rel="absolute", limit:int=10) -> dict:
        raise NotImplementedError


    def count(self, query: dict) -> int:
        raise NotImplementedError


    def remove_first(self, query: dict):
        raise NotImplementedError

    
    def pop_multiple(self, query: dict, limit: int):
        raise NotImplementedError


    def _bulk_save(self, query: dict, data: list):
        raise NotImplementedError


    def single_get(self, query:dict):
        raise NotImplementedError


    def single_set(self, query:dict, data:dict):
        raise NotImplementedError


    def single_delete(self, query:dict):
        raise NotImplementedError