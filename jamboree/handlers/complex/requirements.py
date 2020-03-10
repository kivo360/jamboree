import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.default import DBHandler
from jamboree import Jamboree

class RequirementsHandler(DBHandler):
    """ 
        # RequirementsHandler
        ---


    """
    def __init__(self):
        super().__init__()
        self.entity = "requirements"
        self.required = {
            "req_name": str
        }
        self._episode:str = uuid.uuid4().hex
        self._live:bool = False
    
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
    
    @property
    def specific(self):
        """ Get the requirements specified to the episode and live status"""
        pass


    def reset(self):
        self.check()
        logger.info("Reset information here")
    


if __name__ == "__main__":
    jambo = Jamboree()
    reqhandler = RequirementsHandler()
    reqhandler.event = jambo
    reqhandler.reset()
