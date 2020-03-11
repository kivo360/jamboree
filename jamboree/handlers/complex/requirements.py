import time
import maya
import uuid
from typing import List, Dict, Any
from loguru import logger
from jamboree.handlers.default import DBHandler
from jamboree import Jamboree, JamboreeNew


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
        self._assets:List[str] = []
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

    @property
    def assets(self):
        return self._assets

    @assets.setter
    def assets(self, _assets):
        self._assets = _assets

    @property
    def is_valid(self):
        report_values = list(self._reporting.values())
        if len(report_values) == 0:
            return False
        return all(report_values)

    def report(self, asset:str):
        """ Report that we've passed over an asset """
        self._reporting[asset] = True
        self.update()

    def load(self):
        """ Load all of the assets and the status of those assets as well """
        asset_set = self.last()
        report_set = self.last(alt={"detail": "report"})
        self.assets = asset_set.get("assets", [])
        self._reporting = report_set.get("report", {})

    def update(self):
        self.reset_reporting()
        data = {"assets": self.assets}
        report = {"report": self._reporting}
        self.save(data)
        self.save(report, alt={"detail": "report"})

    def reset_reporting(self):
        for asset in self.assets:
            self._reporting[asset] = self._reporting.get(asset, False)

    def reset(self):
        self.check()
        req_count = self.count()
        if req_count == 0:
            logger.debug("Updating for the first time")
            self.update()
        else:
            self.load()
    


if __name__ == "__main__":
    jambo = JamboreeNew()
    reqhandler = RequirementsHandler()
    reqhandler.processor = jambo
    reqhandler['name'] = uuid.uuid4().hex
    reqhandler.assets = ["BTC", "ABC", "ETH", "ATX"]
    reqhandler.reset()
    

    unique = reqhandler.unique()
    unique = reqhandler.unique()
    unique = reqhandler.unique()
    # print(unique)
