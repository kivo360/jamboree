import uuid
from loguru import logger
import maya
import pandas as pd


from jamboree import JamboreeNew
from jamboree.handlers.complex.backtestable import BacktestDBHandler
from jamboree.handlers.processors import DynamicResample, DataProcessorsAbstract
from jamboree.utils.core import omit



class MetricHandler(BacktestDBHandler):
    """ 
        # METRIC HANDLER
        ---

        A simple metric handler. To be used with all machine learning related functions.

        Given an episode and other crucial information, we'll give all information regarding how something has progressed. 

        Some considerations:

        * We'll load the most recent metrics into the metadata handler (which will let us search for the metric)
        * We want to know how a batch model has been doing between episodes
            * Something that sends aggregation commands for all models that have been touched for a given category, subcategory, name set
            * That would also require us to know how to preprocess that information prior to adding it into the database
            * We'd also want to not do this too often to reduce CPU initialization load
            * A form or rate limiter
        

    """

    def __init__(self):
        super().__init__()
        self.entity = "metric"
        self.required = {
            "category": str,
            "subcategories": dict,
            "name": str
        }
        
        self._preprocessor: DataProcessorsAbstract = DynamicResample("data")
        

    @property
    def preprocessor(self) -> DataProcessorsAbstract:
        return self._preprocessor
    
    @preprocessor.setter
    def preprocessor(self, _preprocessor: DataProcessorsAbstract):
        self._preprocessor = _preprocessor

    def log(self, metric_dict:dict):
        """ Logs a metrics at the current time """
        current_time = self.time.head
        metric_dict['time'] = current_time
        metric_dict['timestamp'] = maya.now()._epoch
        # Add something here to make this searchable as well.
        self.save(metric_dict)
    
    def latest(self):
        """ Get the latest """
        _latest = self.last(ar='relative')
        omitted = omit(['episode', 'mtype', 'live', 'category', 'subcategories', 'type', 'name'], _latest)
        return omitted

    def reset_current_metric(self):
        self['episode'] = self.episode
        self['live'] = self.live

    def reset(self):
        """ Reset the data we're querying for. """
        super().reset()
        self.reset_current_metric()
    
    def step_time(self):
        """ """
        self.time.step()
        pass
    
    
    
    def __str__(self) -> str:
        name = self["name"]
        category = self["category"]
        subcategories = self["subcategories"]
        jscat = self.main_helper.generate_hash(subcategories)
        return f"{name}:{category}:{jscat}"

def metric_test():
    """ Test monitoring an online learning algorithm (using creme). """
    import random
    
    jambo = JamboreeNew()
    metric_log = MetricHandler()
    metric_log['category'] = "model"
    metric_log['subcategories'] = {}
    metric_log['name'] = "general_regressor"
    metric_log.processor = jambo
    metric_log.reset()
    metric_log.time.change_stepsize(hours=0, microseconds=10)
    while True:
        metric_log.reset_current_metric()
        metric_schema = {
            "accuracy": random.uniform(0, 1),
            "f1": random.uniform(0, 1)
        }
        metric_log.log(metric_schema)
        saved_metric = metric_log.latest()
        metric_log.step_time()
        print(saved_metric)

    
if __name__ == "__main__":
    metric_test()
    # import pandas_datareader.data as web
    # data_msft = web.DataReader('MSFT','yahoo',start='2010/1/1',end='2020/1/30').round(2)
    # data_apple = web.DataReader('AAPL','yahoo',start='2010/1/1',end='2020/1/30').round(2)
    # print(data_apple)
    # episode_id = uuid.uuid4().hex
    # jambo = Jamboree()
    # jam_processor = JamboreeNew()
    # data_hander = DataHandler()
    # data_hander.event = jambo
    # data_hander.processor = jam_processor
    # # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    # data_hander.episode = episode_id
    # data_hander.live = False
    # data_hander['category'] = "markets"
    # data_hander['subcategories'] = {
    #     "market": "stock",
    #     "country": "US",
    #     "sector": "techologyyyyyyyy"
    # }
    # data_hander['name'] = "MSFT"
    # data_hander.reset()
    # data_hander.store_time_df(data_msft, is_bar=True)


    # data_hander['name'] = "AAPL"
    # data_hander.store_time_df(data_apple, is_bar=True)
    # data_hander.reset()

    # data_hander.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
    # data_hander.time.change_stepsize(microseconds=0, days=1, hours=0)
    # data_hander.time.change_lookback(microseconds=0, weeks=4, hours=0)

    
    # while data_hander.is_next:
    #     logger.info(magenta(data_hander.time.head, bold=True))
    #     print(data_hander.dataframe_from_head())
    #     data_hander.time.step()