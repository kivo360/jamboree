from loguru import logger
import maya
import uuid
import pandas as pd

from crayons import magenta
from jamboree import Jamboree
from jamboree.base.handlers.time_handler import TimeHandler
from jamboree.base.handlers.main_handler import DBHandler
from jamboree.base.handlers.metadata_handler import MetaHandler
import pandas_datareader.data as web
# from pandas import Series, DataFrame, Timestamp
from pprint import pprint

class DataHandler(DBHandler):
    """ 
        DATA HANDLER
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
        self._time = TimeHandler()
        self._meta = MetaHandler()
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

    @property
    def time(self) -> 'TimeHandler':
        self._time.event = self.event
        self._time['episode'] = self.episode
        self._time['live'] = self.live
        return self._time
    
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
        
        next_data = self.previous_head()
        next_keys = list(next_data.keys())
        # logger.info(magenta(next_data.keys(), bold=True))
        
        # head = self.close_head()
        if len(next_keys) == 0:
            return False
        return True


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
    
    def close_head(self):
        """ Get the closest information at the given head"""
        head = self.time.head
        closest = self.last_by(head, ar="relative")
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

    
if __name__ == "__main__":
    # data = web.DataReader('MSFT','yahoo',start='2010/1/1',end='2020/1/30').round(2)

    episode_id = uuid.uuid4().hex
    jambo = Jamboree()
    data_hander = DataHandler()
    data_hander.event = jambo
    # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    data_hander.episode = episode_id
    data_hander.live = False
    data_hander['category'] = "markets"
    data_hander['subcategories'] = {
        "market": "stock",
        "country": "US",
        "sector": "techologyy"
    }
    data_hander['name'] = "MSFT"
    data_hander.reset()
    # data_hander.store_time_df(data, is_bar=True)

    data_hander.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
    data_hander.time.change_stepsize(microseconds=0, days=1, hours=0)
    data_hander.time.change_lookback(microseconds=0, weeks=4, hours=0)
    
    while data_hander.is_next:
        # logger.info(magenta(, bold=True))
        print(data_hander.dataframe_from_head())
        data_hander.time.step()