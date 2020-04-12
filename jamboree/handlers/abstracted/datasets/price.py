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

class PriceData(DataHandler):
    """
        # Price Data
        
        A way to browse and interact with price data. Is an extension of DataHandler and includes basic searches.

    """
    def __init__(self):
        super().__init__()
        self['category'] = "markets"
        self['submetatype'] = "price"
        self.sc = "subcategories" # storing the placeholder key to prevent misspelling
        # self.cat = "category" # storing variable placeholder key to prevent misspelling

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
        _search[self.sc] = {
            "country": country,
            # "data"
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
        _search["name"] = name
        return _search.find()
    

    def by_exchange(self, name:str):
        if not isinstance(name, str):
            logger.error("The sector should be a string.")
            return []
        _search = self.search
        _search[self.sc] = {
            "exchange": name
        }
        return _search.find()

    
    def multi_search(self, name=None, country=None, sector=None, market=None, exchange=None, is_exact_subcategory=False):
        """ Search with our conventional parameters for our pricing datasets """
        all_variables = {"name": name, "country": country, "sector": sector, "market": market, "exchange": exchange}
        _name = None
        _subcat_dict = {}
        for k, v in all_variables.items():
            if v is None:
                continue
            if k == "name":
                _name = v
                continue
            if k == "market":
                if v not in self.markets:
                    continue
            
            if is_exact_subcategory:
                _subcat_dict[k] = querying.text.exact(v)
            else:
                _subcat_dict[k] = v
        
        is_size = (len(_subcat_dict) == 0)
        is_name = (_name is None)
        if is_size and is_name:
            return []

        _search = self.search

        if not is_name:
            _search["name"] = name
        if not is_size:
            _search[self.sc] = _subcat_dict
        _search.processor = self.processor
        return _search.find()
    

    def build(self, name:str, abbv:str, country:str="US", sector:str="tech", market:str="stock", exchange:str="binance"):
        self['name'] = name
        self['abbreviation'] = abbv
        self['subcategories'] = {
            "market": market,
            "country": country,
            "sector": sector,
            "exchange": exchange,
        }
        return self

    # def get(self, name=None, country=None, sector=None, )

def main():
    import pandas_datareader.data as web
    # data_msft = web.DataReader('MSFT','yahoo',start='2019/9/1',end='2020/1/30').round(2)
    # data_apple = web.DataReader('AAPL','yahoo',start='2019/9/1',end='2020/1/30').round(2)
    episode_id = uuid.uuid4().hex
    jambo = Jamboree()
    jam_processor = Jamboree()
    data_hander = PriceData()
    data_hander.processor = jam_processor
    trx_tron = data_hander.build("Tron", "TRX", country="Japan", sector="oil", market="commodities", exchange="binance")
    # The episode and live parameters are probably not good for the scenario. Will probably need to switch to something else to identify data
    trx_tron.episode = episode_id
    trx_tron.live = False
    trx_tron.reset()
    

    res = trx_tron.multi_search(country="jap")
    pprint.pprint(res)


if __name__ == "__main__":
    main()