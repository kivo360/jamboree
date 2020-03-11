import uuid
from loguru import logger
from crayons import magenta

import maya
import pandas as pd

import ujson
from jamboree import Jamboree
from jamboree import JamboreeNew
from jamboree.handlers.default.db import DBHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.handlers.default.time import TimeHandler
from jamboree.handlers.processors import DynamicResample, DataProcessorsAbstract

class DataHandler(DBHandler):
    """ 
        # DATA HANDLER
        ---

        The data handler does the following:
        
        1. Accepts DataFrame input with or without time index
        2. Converts and saves the data
        3. Attaches to Metadata (needs to have a metadata handler to ensure effective usage of information)
        4. Connects to a MultiDataHandler
            * The MultiDataHandler
        5. Adds autoresampling functionality.

    """

    def __init__(self):
        super().__init__()
        self.entity = "data_management"
        self.required = {
            "category": str,
            "subcategories": dict,
            "name": str,
        }
        self._time:TimeHandler = TimeHandler()
        self._meta:MetaHandler = MetaHandler()
        self._episode = uuid.uuid4().hex
        self._is_live = False
        self._preprocessor:DataProcessorsAbstract = DynamicResample("data")
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
    
    @property
    def metadata(self):
        self._meta.event = self.event
        self._meta['category'] = self['category']
        self._meta.subcategories = self['subcategories']
        self._meta.name = self['name']
        return self._meta
    
    @property
    def is_next(self) -> bool:
        """ A boolean that determines if there's anything next """
        
        next_data = self.closest_head()
        next_keys = list(next_data.keys())
        if len(next_keys) == 0:
            return False
        return True

    @property
    def preprocessor(self) -> DataProcessorsAbstract:
        return self._preprocessor
    
    @preprocessor.setter
    def preprocessor(self, _preprocessor: DataProcessorsAbstract):
        self._preprocessor = _preprocessor


    def _timestamp_resample_and_drop(self, frame: pd.DataFrame, resample_size="D"):
        timestamps = pd.to_datetime(frame.time, unit='s')
        frame.set_index(timestamps, inplace=True)
        frame = frame.drop(columns=["timestamp", "type", "subcategories", "category", "time"])
        frame = frame.resample(resample_size).mean()
        frame = frame.fillna(method="ffill")
        return frame


    def store_time_df(self, dataframe:pd.DataFrame, is_bar=False):
        """ 
            Breaks a dataframe into parts then stores them into redis. 
            Use to handle time series data. Dataframe must have time index
        """
        storable_list = self.main_helper.convert_dataframe_to_storable_item_list(dataframe)
        if is_bar == True:
            storable_list = self.main_helper.standardize_outputs(storable_list)
        self.save_many(storable_list)

    def dataframe_from_head(self):
        """ Get a dataframe between a head and tail. Resample according to our settings"""
        
        head = self.time.head
        tail = self.time.tail
        values = self.in_between(tail, head, ar="relative")
        frame = pd.DataFrame(values)
        frame = self._timestamp_resample_and_drop(frame)
        return frame
    
    def closest_head(self):
        """ Get the closest information at the given head"""
        head = self.time.head
        closest = self.last_by(head, ar="relative")
        closest.pop("name", None)
        closest.pop("category", None)
        closest.pop("subcategories", None)
        closest.pop("type", None)
        return closest
    
    def previous_head(self):
        """ Get the closest information at the given head"""
        head = self.time.peak_back()
        closest = self.last_by(head, ar="relative")
        return closest

    def reset(self):
        """ Reset the data we're querying for. """
        self.metadata.reset()
        self.time.reset()
    
    
    
    def __str__(self) -> str:
        name = self["name"]
        category = self["category"]
        subcategories = self["subcategories"]
        jscat = self.main_helper.generate_hash(subcategories)
        return f"{name}:{category}:{jscat}"

    
if __name__ == "__main__":
    import pandas_datareader.data as web
    data_msft = web.DataReader('MSFT','yahoo',start='2010/1/1',end='2020/1/30').round(2)
    data_apple = web.DataReader('AAPL','yahoo',start='2010/1/1',end='2020/1/30').round(2)
    print(data_apple)
    episode_id = uuid.uuid4().hex
    jambo = Jamboree()
    jam_processor = JamboreeNew()
    data_hander = DataHandler()
    data_hander.event = jambo
    data_hander.processor = jam_processor
    # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    data_hander.episode = episode_id
    data_hander.live = False
    data_hander['category'] = "markets"
    data_hander['subcategories'] = {
        "market": "stock",
        "country": "US",
        "sector": "techologyyyyyyyy"
    }
    data_hander['name'] = "MSFT"
    data_hander.reset()
    data_hander.store_time_df(data_msft, is_bar=True)


    data_hander['name'] = "AAPL"
    data_hander.store_time_df(data_apple, is_bar=True)
    data_hander.reset()

    data_hander.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
    data_hander.time.change_stepsize(microseconds=0, days=1, hours=0)
    data_hander.time.change_lookback(microseconds=0, weeks=4, hours=0)

    
    while data_hander.is_next:
        logger.info(magenta(data_hander.time.head, bold=True))
        print(data_hander.dataframe_from_head())
        data_hander.time.step()