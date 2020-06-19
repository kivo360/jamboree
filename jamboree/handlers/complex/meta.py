import time
import uuid
from typing import Optional

import maya
from jamboree import Jamboree
from jamboree.handlers.abstracted.search import MetadataSearchHandler
from jamboree.handlers.default import DBHandler, Access
from loguru import logger


class MetaHandler(Access):
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
        self.description: Optional[str] = None

    @property
    def search(self):
        metatype = self.metatype
        submetatype = self.submetatype
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
        self._search['name']            = self.name
        self._search['category']        = self.category
        self._search['subcategories']   = self.subcategories
        self._search['abbreviation']    = self.abbreviation
        self._search.processor          = self.processor
        return self._search

    def reset(self):
        self.check()
        qo = self.setup_query()
        qo.pop("mtype", None)
        if self.description is not None:
            qo['description'] = self.description
        
        return self.search.Create(allow_duplicates=False,
                                  no_overwrite_must_have=True,
                                  **qo)


if __name__ == "__main__":
    jambo = Jamboree()
    metahandler = MetaHandler()
    metahandler.event = jambo
    metahandler.reset()
