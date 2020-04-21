import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.default import DBHandler
from jamboree import Jamboree
from jamboree.handlers.abstracted.search import MetadataSearchHandler

class MetaHandler(DBHandler):
    """ 
        # MetaDataHandler
        --- 
        Metadata is "data that provides information about other data".
        
        The MetaHandler is a way to interact with metadata on each data source we have. 
        
        
        It should be used with both the DataHandler an MultDataHandler. As well as any other form of common data we're looking for as well. 
        It would be there in the event that we would want to figure out properties of data without being forced to directly open the data.
        It should also give us the capacity to search for various bits of information (redis_search) in the near future.

        Some usecases of the metadata could include:

        1. Knowing the type of data we're looking at given some information.
            - Time-series
            - Machine Learning Model
            - Network/Graph Data
            - Events
            - Log Data
            - Meta Record
                - A metarecord is a json representative to a complex datatype. 
        2. Times the data was initiatied
        3. Time the data was last modified
            - Modifications can be as simple as: 
                - Adding a new ticker or bar for price information
                - Partial-Fitting a machine learning model
                - Adjusting a weight to a variable
        4. Getting the number of records for a given piece of information
            - Very useful if we're trying to plan around how much we're going to do for a piece of information
        5. Determining if such data exist
            - We would simply create a complex hash function that's pulled from all dbhandlers representing that data type. 
        6. Start and End Time for a given set of records
        7. Location information
            - There can be different location information for each piece of information.
            - Examples:
                - Image weather data
                - Market location data
                - Social interaction location data
                - Login, logout location data
            - Creating something flexible for this would probably be a good idea. 
        

    """
    def __init__(self):
        super().__init__()
        self.entity = "metadata"
        self.required = {
            "name": str,
            "category": str,
            "metatype": str,
            "submetatype": str,
            "abbreviation": str,
            "subcategories": dict
        }
        self._search = MetadataSearchHandler()
        self._settings = {}
        self.is_auto = False
    
    @property
    def search(self):
        metatype = self['metatype']
        submetatype = self['submetatype']
        self._search.entity = self.entity
        self._search['metatype'] = {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "is_exact": True,
                "term": metatype
            }
        }
        self._search['submetatype'] = {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "is_exact": True,
                "term": submetatype
            }
        }
        self._search['name'] = self['name']
        self._search['category'] = self['category']
        self._search['subcategories'] = self['subcategories']
        self._search['abbreviation'] = self['abbreviation']
        self._search.processor = self.processor
        return self._search

    @property
    def settings(self):
        """ Get the latest settings for the item (how to access, how to interact, etc) """
        if self.is_auto: self.load_settings()
        return self._settings
    
    @settings.setter
    def settings(self, _settings):
        self._settings = _settings
        if self.is_auto: self.save_settings()

    def count_settings(self) -> int:
        alt = {"detail": "settings"}
        return self.count(alt=alt)

    def save_settings(self):
        alt = {"detail": "settings"}
        self.save(self._settings, alt=alt)
    
    def load_settings(self):
        alt = {"detail": "settings"}
        self._settings = self.last(self._settings, alt=alt)
        return self._settings

    def _reset_settings(self):
        """ If the metadata has a count of zero, add the settings we've inputted """
        if self.count_settings() == 0:
            self.save_settings()
        else:
            self.load_settings()
    
    
    def reset(self):
        self.check()
        self._reset_settings()
        return self.search.insert(allow_duplicates=False)
    


if __name__ == "__main__":
    jambo = Jamboree()
    metahandler = MetaHandler()
    metahandler.event = jambo
    metahandler.reset()
