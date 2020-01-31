import copy
import maya
import pandas as pd
from abc import ABC, ABCMeta
from typing import Dict, Any, List

from loguru import logger
from crayons import blue, yellow, green
from jamboree.base.processor import EventProcessor
# from .base import BaseHandler
from jamboree.utils.helper import Helpers
from .time_handler import TimeHandler
from .main_handler import DBHandler



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
    
    

    @property
    def time(self) -> 'TimeHandler':
        return _time
    

    @time.setter
    def time(self, _time: 'TimeHandler'):
        self._time = _time
    

    def store_time_df(self, dataframe:pd.DataFrame):
        """ 
            Breaks a dataframe into parts then stores them into redis. 
            Use to handle time series data. Dataframe must have time index
        """
        storable_list = self.main_helper.convert_to_storable_json_list(dataframe)
        self.save_many(storable_list)

    
if __name__ == "__main__":
    simp_data_handler = DataHandler()
    simp_data_handler.many_by(maya.now()._epoch)
    # simp_data_handler[0]
    # simp_data_handler["poop"]