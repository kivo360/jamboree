"""
    Temporarily stores all keys that we'll possibly be using later.

    Entirely used to get the subdocuments by id. 
    Store all subdocuments by super_id


"""

from addict import Dict

class Keystore(object):
    def __init__(self):
        self.store = Dict()
    

    def add_by_superid(self, superid:str, key:str, _dict:dict):
        # _dict.pop("super_id", None) 
        _dict.pop("id", None)
        _dict.pop("payload", None)

        item = {
            key: _dict
        }
        super_item = {
            str(superid): item
        }
        self.store.update(super_item)
    
    def get_by_superid(self, superid:str):
        if superid in self.store:
            return self.store[superid]
        return {}
    

    def add(self, superid:str, key:str, _dict:dict):
        _dict.pop("super_id", None) 
        _dict.pop("id", None)
        _dict.pop("payload", None)

        item = {
            key: _dict
        }
        super_item = {
            str(superid): item
        }
        self.store.update(super_item)
    
    def get(self, superid:str):
        if superid in self.store:
            return self.store[superid]
        return {}
    
    def reset(self):
        self.store = Dict()