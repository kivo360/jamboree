import uuid
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import maya
import pprint
from loguru import logger
from typing import List
from jamboree.handlers.default import DataHandler
from jamboree import Jamboree
from jamboree.utils.support.search import querying
# from jamboree.handlers.abstracted.search.meta import MetaDataSearchHandler
class OrderbookData(DataHandler):
    """
        # Orderbook Data
        
        A way to browse and interact with price data. Is an extension of DataHandler and includes basic searches.

    """
    def __init__(self):
        super().__init__()
        self['subtype'] = "orderbook" 
        self['category'] = "markets"

        self.sc = "subcategories" # storing the placeholder key to prevent misspelling
        self.cat = "category" # storing variable placeholder key to prevent misspelling

    @property
    def markets(self) -> List[str]:
        return [
            'crypto', 'stock', 'commodities', 'forex', 'simulation'
        ]


    def by_market(self, market_type:str):
        """
            # Find All Datasets By Market

            market_type: ['crypto', 'stock', 'commodities', 'forex', 'simulation']
        """
        if market_type not in self.markets:
            logger.error(f"Not the correct type: {market_type} must be {self.markets}")
            return []

        _search = self.search
        _search[self.cat] = "markets"
        _search[self.sc] = {
            "market": market_type
        }
        # print(_search.query_builder.build())
        return _search.find()
    
    def by_country(self, country:str):

        """
            # Find All Datasets By Country

        """

        if not isinstance(country, str):
            logger.error("The country is not the string.")
            return []
        _search = self.search
        _search[self.cat] = "markets"
        _search[self.sc] = {
            "country": country
        }
        return _search.find()
    
    def by_sector(self, sector:str):
        """
            # Find All Datasets By Sector

        """


        if not isinstance(sector, str):
            logger.error("The sector should be a string.")
            return []
        _search = self.search
        _search[self.cat] = "markets"
        _search[self.sc] = {
            "sector": sector
        }
        return _search.find()
    
    def by_name(self, name:str):
        """
            # Find All Datasets By Sector

        """


        if not isinstance(name, str):
            logger.error("The sector should be a string.")
            return []
        _search = self.search
        _search[self.cat] = "markets"
        _search["name"] = name
        return _search.find()
    
    
    
    def multi_part_search(self, name=None, country=None, sector=None, market=None, exchange=None):
        """  """
        all_variables = {"name": name, "country": country, "sector": sector, "market": market, "exchange": exchange}
        _name = None
        _subcat_dict = {}
        for k, v in all_variables.items():
            if v is None:
                continue
            if k == "name":
                _name = v
                continue

            _subcat_dict[k] = querying.text.exact(v)
        
        is_size = (len(_subcat_dict) == 0)
        is_name = (_name is None)
        if is_size and is_name:
            return []

        _search = self.search
        # _search.reset()

        if not is_name:
            logger.warning(name)
            _search["name"] = name
        _search[self.cat] = "markets"
        if not is_size:
            _search[self.sc] = _subcat_dict
        _search.processor = self.processor
        return _search.find()

def main():
    import pandas_datareader.data as web
    # data_msft = web.DataReader('MSFT','yahoo',start='2019/9/1',end='2020/1/30').round(2)
    # data_apple = web.DataReader('AAPL','yahoo',start='2019/9/1',end='2020/1/30').round(2)
    episode_id = uuid.uuid4().hex
    jambo = Jamboree()
    jam_processor = Jamboree()
    data_hander = OrderbookData()
    data_hander.processor = jam_processor
    data_hander.event = jambo
    # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    data_hander.episode = episode_id
    data_hander.live = False
    data_hander['subcategories'] = {
        "market": "stock",
        "country": "Mexico",
        "sector": "tech",
        "exchange": "binance",
    }
    data_hander['name'] = "ETH Ethereum"
    data_hander.reset()
    # data_hander.store_time_df(data_msft, is_bar=True)


    data_hander['name'] = "BTC Bitcoin"
    # data_hander.reset()
    # data_hander.store_time_df(data_apple, is_bar=True)

    start = maya.now()._epoch
    # res1 = data_hander.by_name("Bitcoin")

    # logger.debug(res1)
    end = maya.now()._epoch
    logger.info(end-start)

    res = data_hander.multi_part_search(market="stock", exchange="binance", country="Mexico")
    logger.warning(res)
    
    # _search = data_hander.search
    # _search['subcategories'] = {
    #     "market": "stock"
    # }
    # _search.remove()
    # data_hander.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
    # data_hander.time.change_stepsize(microseconds=0, days=1, hours=0)
    # data_hander.time.change_lookback(microseconds=0, weeks=4, hours=0)

    
    # while data_hander.is_next:
    #     logger.debug(data_hander.time.head)
    #     print(data_hander.closest_head())
    #     data_hander.time.step()


if __name__ == "__main__":
    main()