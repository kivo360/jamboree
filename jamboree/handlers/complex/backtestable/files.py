import uuid
from loguru import logger

import maya
import pandas as pd

from jamboree import Jamboree
from jamboree import JamboreeNew

from jamboree.handlers.complex.meta import MetaHandler
# from jamboree.handlers.complex.metric import MetricHandler

from jamboree.handlers.default.time import TimeHandler
from jamboree.handlers.default.db import DBHandler
from jamboree.handlers.default import BlobStorageHandler

class BacktestBlobHandler(BlobStorageHandler):
    def __init__(self):
        super().__init__()
        
        
        # Other objects to consider
        self._time:TimeHandler = TimeHandler()
        self._meta:MetaHandler = MetaHandler()
        self._episode = uuid.uuid4().hex
        
        
        self._is_live = False
        self.is_event = False # use to make sure there's absolutely no duplicate data
    
    # Create a context for this to remove complete dependency from the model handler.
    
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

    @property
    def time(self) -> 'TimeHandler':
        # self._time.event = self.event
        self._time.processor = self.processor
        self._time['episode'] = self.episode
        self._time['live'] = self.live
        return self._time

    @time.setter
    def time(self, _time:'TimeHandler'):
        self._time = _time


    def reset(self):
        """ Reset the data we're querying for. """
        self.time.reset()
    
    
    
    def __str__(self) -> str:
        name = self["name"]
        category = self["category"]
        subcategories = self["subcategories"]
        jscat = self.main_helper.generate_hash(subcategories)
        return f"{name}:{category}:{jscat}"