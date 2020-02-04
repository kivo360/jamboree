import uuid
import time
import maya
import uuid
from typing import List, Dict, Any
from jamboree.base.handlers.main_handler import DBHandler
from jamboree import Jamboree

class MultiDataManagement(DBHandler):
    """ 
        # Multi-Data Handler
        ---

        Here we define how different data sources will be pulled at the same time. 
        
        Given a set of parameters we can select which data sources we want to pull for a given problem. 


        ## Why is this useful?
        ---
        This is useful because we're able to pull together any number of data sources we have stored inside of our database.

        Some usecases of this would be:

        - Getting multiple assets to define a portfolio allocation.
        - Getting different asset classes define pairs to trade on.
        - Getting a superset of different asset classes to predict movements between them.
        - Getting a predefied pair to determine which signals should be used.
    """
    def __init__(self):
        super().__init__()
        self.entity = "multi_data_management"
        self.required = {
            "identifier": str
        }
        self._episode = uuid.uuid4().hex
        self._is_live = False
        self.is_event = False # use to make sure there's absolutely no duplicate data
    
    @property
    def episode(self) -> str:
        return self._episode
    
    @episode.setter
    def episode(self, _episode:str):
        self._episode = _episode
    
    @property
    def live(self) -> bool:
        return self._is_live
    
    @live.setter
    def live(self, _live:bool):
        self._is_live = _live

    def add_multiple_data_sources(self, sources: List[Dict[str, Any]]):
        pass

    def add_data_source(self, source:Dict[str, Any]):
        source_list = [source]
        self.add_multiple_data_sources(source_list)

    def _save_random(self):
        """ Use to test that the save function works"""
        self.check()
        self.save({"one": "two"}, alt={"field": uuid.uuid4().hex})

    def reset(self):
        """ Reset the data we're querying for. """
        self._save_random()





if __name__ == "__main__":
    jam = Jamboree()



