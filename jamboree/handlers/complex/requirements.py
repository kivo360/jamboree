import time
import maya
import uuid
from typing import List, Dict, Any, Set
from loguru import logger
from jamboree import Jamboree, JamboreeNew
from jamboree.handlers.default import DBHandler
from jamboree.utils.helper import Helpers

helpers = Helpers()

class RequirementsHandler(DBHandler):
    """ 
        # RequirementsHandler
        ---


    """
    def __init__(self):
        super().__init__()
        self.entity = "requirements"
        self.required = {
            "name": str
        }
        self._episode:str = uuid.uuid4().hex
        self._live:bool = False
        self._assets:List[Dict[Any, Any]] = []
        self._items_compressed:Set[str] = set() #
        self._reporting = {}
    
    @property
    def episode(self):
        return self._episode
    

    @episode.setter
    def episode(self, _episode):
        self._episode = _episode
    
    @property
    def live(self):
        return self._live

    @live.setter
    def live(self, _live):
        self._live:bool = _live
    
    def unique(self):
        """ Get the requirements specified to the episode and live status"""
        unique_item = RequirementsHandler()
        unique_item['name'] = self['name']
        unique_item['episode'] = self.episode
        unique_item['live'] = self.live
        unique_item.assets = self.assets
        unique_item.processor = self.processor
        unique_item.reset()
        return unique_item

    
    def asset_update(self, items:List[Dict[Any, Any]], command="add"):
        if len(items) == 0:
            return
        
        if command not in ["add", "sub"]: return
        
        _current_items = set()
        [_current_items.add(helpers.generate_hash(x)) for x in items]
        old_items = self.items
        if command == "add":
            old_items = old_items.union(_current_items)
        else:
            old_items = old_items.difference(_current_items)
        print(len(old_items))
        self.items = old_items
        # self.assets = self.decomp_items
        self.update()



    @property
    def assets(self):
        return self._assets

    @assets.setter
    def assets(self, _assets):
        self._assets = _assets
        self.assets_to_items()

    @property
    def items(self) -> Set:
        return self._items_compressed
    
    @items.setter
    def items(self, _items:Set[str]):
        self._items_compressed = _items

    @property
    def decomp_items(self):
        if len(self.items) == 0: return []
        return [helpers.hash_to_dict(x) for x in self.items]


    @property
    def is_valid(self):
        report_values = list(self._reporting.values())
        if len(report_values) == 0:
            return False
        return all(report_values)

    def report(self, asset:Dict[Any, Any]):
        """ Report that we've passed over an asset """
        itemized = helpers.generate_hash(asset)
        self._reporting[itemized] = True
        self.update()



    def load(self):
        """ Load all of the assets and the status of those assets as well """
        asset_set = self.last()
        report_set = self.last(alt={"detail": "report"})
        self.assets = asset_set.get("assets", [])
        self._reporting = report_set.get("report", {})
        self.general_update()

    def assets_to_items(self):
        _item_list = set()
        for asset in self.assets:
            _item_list.add(helpers.generate_hash(asset))
        self._items_compressed = _item_list
    
    def items_to_assets(self):
        pass

    def update(self):
        self.general_update()
        data = {"assets": self.assets, "time": time.time(), "timestamp": time.time()}
        report = {"report": self._reporting, "time": time.time(), "timestamp": time.time()}
        self.save(data, alt={})
        # print(count)
        self.save(report, alt={"detail": "report"})

    def reset_reporting(self):
        for item in self.items:
            self._reporting[item] = self._reporting.get(item, False)
    
    def general_update(self):
        self.assets = self.decomp_items
        self.reset_reporting()

    def reset(self):
        req_count = self.count()
        if req_count == 0:
            logger.debug("Updating for the first time")
            self.update()
        else:
            self.load()
    


if __name__ == "__main__":
    
    all_assets = ["BTC", "XTZ", "ETH", "ATX", "XRP", "BCH", "BSV", "LTC", "EOS"]

    jambo = JamboreeNew()
    reqhandler = RequirementsHandler()
    reqhandler.processor = jambo
    # The name is on rick and morty so it's not perverted. I can prove it
    reqhandler['name'] = "Poopybutthole"
    reqhandler.assets = [
        {
            "name": x, 
            "category": "market", 
            "subcategories": {"exchange": "fake_exchange"}
        } 
        for x in all_assets
    ]
    reqhandler.reset()
    new_items = [
        {
            "name": x, 
            "category": "market", 
            "subcategories": {"exchange": "fake_exchange"}
        } 
        for x in ["ONE", "TWO", "THREE", "FOUR", "BTC", "ATX"]
    ]
    reqhandler.asset_update(new_items, command="sub")
    print(len(reqhandler.assets))
    reqhandler.reset()