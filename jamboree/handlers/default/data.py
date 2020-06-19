import uuid

import maya
import pandas as pd
import ujson
from loguru import logger

from jamboree import Jamboree
from jamboree.handlers.abstracted.search import MetadataSearchHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.handlers.default import Access, DBHandler, TimeHandler
from jamboree.handlers.processors import (DataProcessorsAbstract,
                                          DynamicResample)
from jamboree.utils import omit
from jamboree.utils.support.search import querying


class DataHandler(Access):
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
        self.entity = "data"

        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict,
            "metatype": str,
            "submetatype": str,
            "abbreviation": str,
        }
        self._time: TimeHandler = TimeHandler()
        self._meta: MetaHandler = MetaHandler()
        self._metasearch = MetadataSearchHandler()
        self._episode = uuid.uuid4().hex
        self._is_live = False
        self._preprocessor: DataProcessorsAbstract = DynamicResample("data")
        self.is_event = False  # use to make sure there's absolutely no duplicate data
        self.metaid = ""
        self["metatype"] = self.entity

        self.is_robust = False

    @property
    def episode(self) -> str:
        return self._episode

    @episode.setter
    def episode(self, _episode: str):
        self._episode = _episode

    @property
    def live(self) -> bool:
        return self._is_live

    @live.setter
    def live(self, _live: bool):
        self._is_live = _live

    @property
    def time(self) -> "TimeHandler":
        self._time.processor = self.processor
        self._time["episode"] = self.episode
        self._time["live"] = self.live
        return self._time

    @time.setter
    def time(self, _time: "TimeHandler"):
        self._time = _time

    @property
    def metadata(self):
        self._meta.processor = self.processor
        self._meta["name"] = self.name
        self._meta["category"] = self.category
        self._meta["subcategories"] = self.subcategories
        self._meta["metatype"] = self.entity
        self._meta["submetatype"] = self.submetatype
        self._meta["abbreviation"] = self.abbreviation
        return self._meta

    @property
    def search(self):
        self._metasearch.reset()
        self._metasearch["category"] = querying.text.exact(self.category)
        self._metasearch["metatype"] = querying.text.exact(self.entity)
        self._metasearch["submetatype"] = querying.text.exact(self.submetatype)

        self._metasearch.processor = self.processor
        return self._metasearch

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
        timestamps = pd.to_datetime(frame.time, unit="s")
        frame.set_index(timestamps, inplace=True)
        frame = frame.drop(
            columns=["timestamp", "type", "subcategories", "category", "time"]
        )
        frame = frame.resample(resample_size).mean()
        frame = frame.fillna(method="ffill")
        return frame
    def store_time_df(self, dataframe: pd.DataFrame, is_bar=False):
        """ 
            Breaks a dataframe into parts then stores them into redis. 
            Use to handle time series data. Dataframe must have time index
        """
        storable_list = self.main_helper.convert_dataframe_to_storable_item_list(
            dataframe
        )
        if is_bar == True:
            storable_list = self.main_helper.standardize_outputs(storable_list)
        self.save_many(storable_list)

    def add_now(self, data_dict: dict, is_bar=False):
        """ Add information to the current dataset"""
        data_dict_list = [data_dict]
        if is_bar == True:
            data_dict_list = self.main_helper.standardize_outputs(data_dict_list)
        self.save_many(data_dict_list)

    def dataframe_from_head(self):
        """ Get a dataframe between a head and tail. Resample according to our settings"""

        head = self.time.head
        tail = self.time.tail
        values = self.in_between(tail, head, ar="relative")
        frame = pd.DataFrame(values)
        frame = self._timestamp_resample_and_drop(frame)
        return frame
    

    def dataframe_all(self):
        """ Get a dataframe between a head and tail. Resample according to our settings"""

        values = self.query_all()
        frame = pd.DataFrame(values)
        frame = self._timestamp_resample_and_drop(frame)
        return frame

    def dataframe_from_dynamic_peak(self, n_head: int = 1, n_tail=100000):
        """ Get a dataframe between a head and tail. Resample according to our settings"""

        head = self.time.peak_back_num(n_head)
        tail = self.time.peak_back_num_tail(n_tail)
        values = self.in_between(tail, head, ar="relative")
        frame = pd.DataFrame(values)
        frame = self._timestamp_resample_and_drop(frame)
        return frame

    def dataframe_from_last(self):
        """ Get a dataframe with all of the last information. Resample according to our settings"""

        values = self.many(ar="relative")
        frame = pd.DataFrame(values)
        frame = self._timestamp_resample_and_drop(frame)
        return frame

    def closest_head(self, is_robust=False):
        """ Get the closest information at the given head. Otherwise get the latest information"""
        head = self.time.head
        count = self.count()
        closest = self.last_by(head, ar="relative")
        if len(closest) == 0:
            if (is_robust or self.is_robust) and count > 0:
                last = self.last(ar="relative")
                last.pop("name", None)
                last.pop("mtype", None)
                last.pop("category", None)
                last.pop("subcategories", None)
                last.pop("type", None)
                return last
            return {}
        closest.pop("name", None)
        closest.pop("category", None)
        closest.pop("subcategories", None)
        closest.pop("type", None)
        return closest

    def closest_peakback_by(self, n: int = 1, is_robust=False):
        """ Get the closest information at the given head. Otherwise get the latest information"""
        head = self.time.peak_back_num(n)
        count = self.count()
        closest = self.last_by(head, ar="relative")
        if len(closest) == 0:
            if is_robust or self.is_robust:
                if count > 0:
                    last = self.last(ar="relative")
                    last.pop("name", None)
                    last.pop("mtype", None)
                    last.pop("category", None)
                    last.pop("subcategories", None)
                    last.pop("type", None)
                    return last
                return {}
        closest.pop("name", None)
        closest.pop("category", None)
        closest.pop("subcategories", None)
        closest.pop("type", None)
        return closest

    def closest_head_omitted(self, is_robust=False):
        """ Get the closest information at the given head. Otherwise get the latest information"""
        head = self.time.head
        count = self.count()
        closest = self.last_by(head, ar="relative")
        omit_list = [
            "metatype",
            "submetatype",
            "abbreviation",
            "mtype",
            "category",
            "subcategories",
            "name",
            "type",
        ]
        if len(closest) == 0:
            if (is_robust or self.is_robust) and count > 0:
                last = self.last(ar="relative")
                last = omit(omit_list, last)
                return last
            return {}

        closest = omit(omit_list, closest)
        return closest

    def closest_peakback_by_omitted(self, n: int = 1, is_robust=False):
        """ Get the closest information at the given head. Otherwise get the latest information"""
        head = self.time.peak_back_num(n)
        count = self.count()
        closest = self.last_by(head, ar="relative")
        omit_list = [
            "metatype",
            "submetatype",
            "abbreviation",
            "mtype",
            "category",
            "subcategories",
            "name",
            "type",
        ]
        if len(closest) == 0:
            if is_robust or self.is_robust:
                if count > 0:
                    last = self.last(ar="relative")
                    last = omit(omit_list, last)
                    return last
                return {}
        closest = omit(omit_list, closest)
        return closest

    def previous_head(self):
        """ Get the closest information at the given head"""
        head = self.time.peak_back()
        closest = self.last_by(head, ar="relative")
        return closest

    def reset(self):
        """ Reset the data we're querying for. """

        # Update. Get the highest and lowest score for the current dataset.
        self.time.reset()
        self.metaid = self.metadata.reset()
        return self.metaid

    def pick(self, _id: str) -> None:
        """Get the metadata to populate the current fields and loads the data.

        Args:
            _id (str): The metadata identifier.

        Raises:
            AttributeError: Throws an attribute error if metadata couldn't be found.
        """
        current_search = self.search
        item = current_search.pick(_id)

        if item is None:
            raise AttributeError("Data source currently doesn't exist.")

        self["name"] = item["name"]
        self["category"] = item["category"]
        self["subcategories"] = item["subcategories"]
        self["metatype"] = item["metatype"]
        self.entity = item["metatype"]
        self["submetatype"] = item["submetatype"]
        self["abbreviation"] = item["abbreviation"]
        self.reset()

    def __str__(self) -> str:
        name: str = self["name"]
        category: str = self["category"]
        subcategories: dict = self["subcategories"]
        metatype: str = self["metatype"]
        submetatype: str = self["submetatype"]
        abbreviation: str = self["abbreviation"]

        jscat = self.main_helper.generate_hash(subcategories)
        return f"{name}:{category}:{jscat}:{metatype}:{submetatype}:{abbreviation}"


if __name__ == "__main__":
    import pandas_datareader.data as web

    data_msft = web.DataReader(
        "MSFT", "yahoo", start="2010/1/1", end="2020/1/30"
    ).round(2)
    data_apple = web.DataReader(
        "AAPL", "yahoo", start="2010/1/1", end="2020/1/30"
    ).round(2)
    # print(data_apple)
    episode_id = uuid.uuid4().hex
    jambo = Jamboree()
    data_hander = DataHandler()
    data_hander.event = jambo
    data_hander.processor = jambo
    # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    data_hander.episode = episode_id
    data_hander.live = False
    data_hander["category"] = "markets"
    data_hander["subcategories"] = {
        "market": "stock",
        "country": "US",
        "sector": "techologyyyyyyyy",
    }
    data_hander["name"] = "MSFT"
    data_hander["submetatype"] = "POOP"
    data_hander["abbreviation"] = "MSFT"
    data_hander.reset()
    data_hander.store_time_df(data_msft, is_bar=True)
    data_hander["name"] = "AAPL"
    data_hander.store_time_df(data_apple, is_bar=True)
    data_hander.reset()

    data_hander.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
    data_hander.time.change_stepsize(microseconds=0, days=1, hours=0)
    data_hander.time.change_lookback(microseconds=0, weeks=4, hours=0)

    while data_hander.is_next:

        logger.success(data_hander.name)
        logger.warning(data_hander.category)
        logger.error(data_hander.subcategories)
        logger.debug(data_hander.abbreviation)
        logger.success(data_hander.submetatype)
        
        print()
        print()
        logger.success(data_hander.time.head)
        logger.info(data_hander.time.head)
        logger.success(data_hander.time.head)
        print()
        print()
        data_hander.time.step()
