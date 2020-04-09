import uuid
from copy import copy
from functools import lru_cache
from pprint import pprint
from typing import List
import random

import maya
import yfinance as yf
from addict import Dict
from anycache import anycache
from loguru import logger
import crayons as cy

from jamboree import Jamboree
from jamboree.handlers import DataHandler, MultiDataManagement
from jamboree.handlers.abstracted.datasets.price import PriceData
import threading


"""
    Find manipulate price information dynamically like you would from a UI
"""


@anycache(cachedir='/tmp/anycache.my')
def get_dataset_info(ds_str:str):
    # ds_str = " ".join(ds_list)
    ticker = yf.Ticker(ds_str)
    
    return ticker.info

@anycache(cachedir='/tmp/anyticker.my')
def get_ticker_data(ds_str:str):
    data = yf.download(ds_str, group_by = 'ticker').dropna()
    return data

# Find all datasets by market
class TestSearchTrial(object):
    """ 
        # TEST SEARCH TRIAL

        Class to test running through various scenarios to:

        1. Download data (apple and msft data)
        2. Save it to given metadata inside of redis
        3. Find all various metadata inside of redis using incomplete information
        4. Get the dataset_ids by subtype match
        5. Get datasets by_ids (resemble putting datasets by)
        6. Add datasets into a multidata container
        7. Backtest through the multiple datasets
    """
    def __init__(self):
        # List of datasets we intend to download from yahoo finance
        self.datalist = ["MSFT", "AAPL", "GOOG"]
        self.datasets = []
        self.dataset_info = {}
        self.processor = Jamboree()
        self.pricedata_management = PriceData()
        self.pricedata_management.processor = self.processor
        self.dow_metadata = []
        self.reloaded_datasets = []
        self.episodes = []
        self._raw = []
        self.set_name = "default_test"
    
        

        # # pass
    
    def download_yfinance_info(self):
        for ds in self.datalist:
            info = get_dataset_info(ds)
            self.dataset_info[ds] = {
                "long_name": info["longName"],
            }


    def download_from_yfinance(self):
        self.ds_str = " ".join(self.datalist)
        logger.info(f"starting download for datasets from yahoo finance {self.ds_str}")
        self.datasets = get_ticker_data(self.ds_str)
        logger.success(f"finished downloading from yahoo finance: {self.ds_str}")
    
    def save_datasets(self):
        """ Save all of the datasets with the given metadata information. """

        for k, v in self.dataset_info.items():
            abbv = k
            long_name = v['long_name']
            df = self.datasets[k]
            self.pricedata_management.build(name=long_name, abbv=abbv, exchange="Dow Jones")
            self.pricedata_management.reset()
            self.pricedata_management.store_time_df(df, is_bar=True)
    
    def find_all_datasets_by_exchange(self):
        name = "do*"
        logger.info(f'Finding metadata by partial exchange query \"{name}\"' )
        self.dow_metadata = self.pricedata_management.by_exchange(name)
        logger.success(f"Successfully loaded metadata ... ")
    
    def find_meta_by_id(self, _id):
        """ Find a single metadata document by id"""
        return self.pricedata_management.search.pick(_id)
    
    def from_meta(self, item:dict):
        """ Gets a dataset from metadata """
        dd = Dict(**item)
        entity = dd.metatype
        name = dd.name
        category = dd.category
        subcategories = dd.subcategories
        submetatype = dd.submetatype # the specific kind of metadata (price, economic)
        abbv = dd.abbreviation

        dataset = DataHandler()
        # dataset.entity = entity
        dataset['name'] = name
        dataset['category'] = category
        dataset['subcategories'] = subcategories
        dataset['submetatype'] = submetatype
        dataset['abbreviation'] = abbv
        dataset.processor = self.processor
        dataset.reset()
        
        return dataset
    
    def raw_dict(self, item:dict):
        dd = Dict(**item)
        norm = Dict()
        name = dd.name
        category = dd.category
        subcategories = dd.subcategories
        submetatype = dd.submetatype # the specific kind of metadata (price, economic)
        abbv = dd.abbreviation
        norm.name = name
        norm.category = category
        norm.subcategories = subcategories
        norm.submetatype = submetatype
        norm.abbreviation = abbv
        return norm.to_dict()

    def get_datasets_by_metadata(self):
        """ Get all of dataset objects by metdata. Used to see if we can """
        data_list:List[DataHandler] = []
        dow_len = len(self.dow_metadata)
        
        if dow_len > 0:
            logger.debug(f"Number of metadata sets: {dow_len}")
            for _ in self.dow_metadata:
                dataset = self.from_meta(_)
                data_list.append(dataset)
                self._raw.append(self.raw_dict(_))

        ll = len(data_list)    
        self.reloaded_datasets = data_list
        logger.success(f"Successfully created datasets ... count: {ll}")
    
    def create_episodes(self):
        """ Create a bunch of episodes """
        self.episodes = [uuid.uuid4().hex for x in range(len(self.reloaded_datasets))]
        for index, episode in enumerate(self.episodes):
            dataset = self.reloaded_datasets[index]
            dataset.episode = episode
            dataset.live = False
            dataset.time.head = maya.now().subtract(weeks=600)._epoch
            dataset.time.change_stepsize(microseconds=0, days=1, hours=0)
            dataset.time.change_lookback(microseconds=0, weeks=4, hours=0)
            self.reloaded_datasets[index] = dataset
    
    def create_multi_data(self):
        self.multi_data = MultiDataManagement()
        self.multi_data["set_name"] = self.set_name
        self.multi_data.processor = self.processor
        self.multi_data.episode = uuid.uuid4().hex
        self.multi_data.reset()
        self.multi_data.add_multiple_data_sources(self._raw)
        pprint(self.multi_data.sources)


    def run_random_dataset(self):
        dataset = random.choice(self.reloaded_datasets)
        start_time = maya.now()._epoch
        i = 0
        while dataset.is_next:
            # This cuts loop time in half immediately
            dataset.time.step()
            data = dataset.closest_head()
            time_diff = maya.now().epoch - start_time
            i += 1
            milli = (1000 / (i / time_diff)) 
            logger.info(f"Number of milliseconds: {milli}")
            # logger.error(data)
    
    def run_multidata(self):
        self.multi_data.time.head = maya.now().subtract(weeks=600, hours=14)._epoch
        self.multi_data.time.change_stepsize(microseconds=0, days=1, hours=0)
        self.multi_data.time.change_lookback(microseconds=0, weeks=4, hours=0)
        self.multi_data.sync()
        start_time = maya.now()._epoch
        i = 0
        while self.multi_data.is_next:
            latest = self.multi_data.step("current")
            self.multi_data.time.step()
            time_diff = maya.now().epoch - start_time
            i += 1
            milli = (1000 / (i / time_diff)) 
            logger.debug(latest)
            logger.info(f"Number of milliseconds: {milli}")
            

    def run(self, is_download=True):

        if is_download:
            # Downloads the data from yahoo finance to our system
            self.download_yfinance_info()
            self.download_from_yfinance()
            self.save_datasets()
        
        # Finds every Dow Jones exchanges and stores metadata locally
        self.find_all_datasets_by_exchange() 
        # Takes local metadata and finds the datasources in the system
        self.get_datasets_by_metadata() 
        # Creates a bunch of backtest episodes (only to iterate through episodes)
        self.create_episodes()
        # Creates a batch dataset (pulling everything at once)
        self.create_multi_data()
        # Picks a random individual dataset and runs through it

        random_pick1 = threading.Thread(target=self.run_random_dataset, args=())
        # random_pick2 = threading.Thread(target=self.run_random_dataset, args=())
        # multiple_pick = threading.Thread(target=self.run_multidata, args=())
        
        random_pick1.start()
        # random_pick2.start()
        # multiple_pick.start()

        random_pick1.join()
        # random_pick2.join()
        # multiple_pick.join()
    
    def run_findselection(self):
        # logger.warning("something is here")
        self.find_all_datasets_by_exchange()
        item = random.choice(self.dow_metadata)
        item = Dict(**item)
        refind = self.find_meta_by_id((item.id))
        # print(refind)
        if refind is None:
            raise AttributeError("item doesn't exist")
        datasource = self.from_meta(refind)
        num = datasource.count()
        last = datasource.closest_head()
        # many = datasource.many()
        logger.warning(num)
        logger.warning(last)
        # logger.warning(many)
    





if __name__ == "__main__":
    trial = TestSearchTrial()
    # trial.run()
    trial.run_findselection()

