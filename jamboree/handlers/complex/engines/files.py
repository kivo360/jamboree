import random
import time
import uuid
from pprint import pprint
from typing import Any, List, Optional

import maya
from addict import Dict
from loguru import logger

from jamboree import Jamboree
from jamboree.handlers.abstracted.search.meta import MetadataSearchHandler
from jamboree.handlers.complex.backtestable import BacktestBlobHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.handlers.complex.metric import MetricHandler
from jamboree.middleware.procedures import (
    ProcedureAbstract,
    ProcedureManagement,
)
from jamboree.utils.support.search import querying

logger.disable('jamboree')


class FileEngine(BacktestBlobHandler):
    """ 
        # FileEngine

        Mixing:
            * Native Backtesting
            * BlobHandler
            * Metrics
            * ContextManager
            * Search

        The goal of this handler is to be able to have something generic you can use for all of our backtesting needs with blobs.

        The idea behind it is that we'd need to have raw blobs to manage things like the following:
            1. Saving and loading strategies
            2. Saving and loading machine learning models
        
        For those two we'll need to do the following:

            1. Search through models
            2. Store metrics for a given file
                * So we can see the performance for a given item over time (such as a machine learning calculation/etc)
            3. Get specific files by ID
            4. Set and get settings of objects
            5. Make sure we don't miss anything
                * Saving metrics for a strategy or model
                * Saving our models and stratgies after using them. 
                    * This is especially important if we haven't seen certain data before.
                * Running other clean up commands at the end.
        
        The core solution for this is to create an integrated setup that we can inherit from and run in a common way.
        
        The second is to use a context manager to load information and use it immediately. 
        
        If we have an online learning algorithm we'd have a `ModelEngine`, which would inherit from the `FileEngine` and open the context manager. 

        We'd then do the work we could do the following command:

            ```
                with compute_engine as model:
                    model.partial_fit(x, y)
            ```
    
    """

    def __init__(self, processor=None, **kwargs) -> None:
        super(FileEngine, self).__init__()
        self.entity = "file_engine"  # Gen model represents general model
        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict,  # other information regarding the library type.
            # Search specific information
            "metatype": str,
            "submetatype": str,
            "abbreviation": str,
        }
        self["metatype"] = self.entity

        if processor is not None:
            self.processor = processor
        self.metaid = ""
        self.initialize(**kwargs)

        self.__name:str = ""
        self.__category:str = ""
        self.__subcategories:dict = {}
        self.__metatype:str = ""
        self.__submetatype:str = ""
        self.__abbreviation:str = ""
    
    @property
    def name(self) -> str:
        """The name property."""
        return self["name"]
    
    @name.setter
    def name(self, value:str):
        self["name"] = value

    @property
    def category(self) -> str:
        """The category property."""
        return self["category"]


    @category.setter
    def category(self, value:str):
        self["category"] = value


    @property
    def subcategories(self) -> dict:
        """The subcategories property."""
        return self["subcategories"]


    @subcategories.setter
    def subcategories(self, value:dict):
        self["subcategories"] = value
    

    @property
    def metatype(self) -> str:
        """The metatype property."""
        return self.entity
    

    @metatype.setter
    def metatype(self, value:str):
        self.entity = value
        self["metatype"] = value
    
    @property
    def submetatype(self) -> str:
        """The submetatype property."""
        return self['submetatype']
    
    @submetatype.setter
    def submetatype(self, value:str):
        self['submetatype'] = value
    

    @property
    def abbreviation(self)->str:
        """The abbreviation property."""
        return self["abbreviation"]

    @abbreviation.setter
    def abbreviation(self, value:str):
        self["abbreviation"] = value

    """ 
        Context Manager

        with compute_engine as model:
            prediction = self.model.fit_predict(data) # we'd have preprocessing strategies
    """

    def __enter__(self):
        self.open_context()
        return self.enterable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.close_context()

    def enterable(self):
        """ Return the object we want to enter into """
        raise NotImplementedError("Enterable not added yet.")

    def open_context(self):
        """ Take the model from the model procedure and save it. """
        raise NotImplementedError("Open context hasn't had anything added")

    def close_context(self):
        """ Take the model from the model procedure and save it. """
        raise NotImplementedError("Close context hasn't had anything added.")

    """
        Initialize everything for the user
    """

    def initialize(self, **kwargs):
        self.init_handlers()
        self.init_settings(**kwargs)
        self.init_required(**kwargs)
        self.init_specialized(**kwargs)

    def init_handlers(self):
        self._metrics: MetricHandler = MetricHandler()
        self._metasearch: MetadataSearchHandler = MetadataSearchHandler()

    def init_settings(self, **kwargs):
        """ All of the general settings that we need to initialize """
        # self.metadata.settings
        self.online = kwargs.get("online", False)
        self.bfile = kwargs.get("blobfile", None)
        self.file_reset = False
        self.searchable_items = kwargs.get(
            "searchable", ["name", "subcategories", "abbreviation"]
        )
        self.pm: Optional[ProcedureManagement] = kwargs.get("proc_management", None)

    def init_required(self, **kwargs):
        if len(kwargs) == 0:
            return
        for k, v in self.required.items():
            if k in kwargs:
                value = kwargs.get(k)
                if isinstance(value, v):
                    self[k] = value

    def init_specialized(self, **kwargs):
        """ Initialize all of the highly specific parts """
        self.current_procedure = None

    """ Properties """

    """ 
        # Handler Properties 
        

        Other handlers to interact with other parts of the system.

        1. `self.metrics` to interact with the metrics (logging)
        2. `self.metadata` to interact with the FileEngine's settings and capacity to find information
        3. `self.search` to handle search specified to the key handler. 

        Each handler has its own information attached.
    """

    @property
    def metrics(self):
        self._metrics.processor = self.processor
        self._metrics["name"] = self["name"]
        self._metrics["category"] = self["category"]
        self._metrics["subcategories"] = self["subcategories"]
        self._metrics.episode = self.episode
        self._metrics.live = self.live
        self._metrics.time = self.time
        self._metrics.reset()
        return self._metrics

    @property
    def metadata(self):
        self._meta.processor        = self.processor
        self._meta["name"]          = self["name"]
        self._meta["category"]      = self["category"]
        self._meta["subcategories"] = self["subcategories"]
        self._meta["metatype"]      = self.entity
        self._meta["submetatype"]   = self['submetatype']
        self._meta["abbreviation"]  = self["abbreviation"]
        return self._meta

    @property
    def search(self) -> MetadataSearchHandler:
        self._metasearch.reset()
        self._metasearch["category"] = querying.text.exact(self["category"])
        self._metasearch["metatype"] = querying.text.exact(self.entity)
        self._metasearch["submetatype"] = querying.text.exact(self["submetatype"])
        self._metasearch.processor = self.processor
        return self._metasearch

    """ 
        # Key Variables 
        ---

        The core variables to handle the functionality of the file engine. 

        1. Checking to see if a file is in storage
        2. Checking to see if we've modified a variable since interacting with storage
        3. Setting verification methods and procedure management
    """

    @property
    def is_exist(self) -> bool:
        """ Checks to see if a model exist in storage."""
        existence = self.absolute_exists()
        return existence

    @property
    def is_exist_forced(self) -> bool:
        """ Checks to see if a model exist in storage."""
        self.changed_since_command = True
        existence = self.absolute_exists()
        return existence

    @property
    def changed(self) -> bool:
        return self.changed_since_command
    @property
    def procedure(self) -> Optional["ProcedureAbstract"]:
        raise NotImplementedError("Procedure has not been implemented")

    @procedure.setter
    def procedure(self, _procedure: "ProcedureAbstract"):
        """ Set the access procedure"""
        self.current_procedure = _procedure

    @property
    def is_procedure(self):
        return self.current_procedure is not None

    @property
    def is_proc(self) -> bool:
        """ Checks to see if procedure has been set"""
        return not self.current_procedure

    @property
    def is_local_file(self) -> bool:
        """ Boolean explaining if we've set a file locally. Files can be set either through pulling from the DB or manually"""
        # if the blob file has been set locally, set the response to true
        return self.blobfile is not None
    @property
    def blobfile(self):
        return self.bfile

    @blobfile.setter
    def blobfile(self, _file):
        self.bfile = _file

    """
        # Database Commands

        * Save file
        * Load file
    """

    def verify(self):
        self.pm.isattr(self)


    def load_file(self):
        loaded_file = self.last()
        return loaded_file

    def save_file(self, _file: Any, alt={}):
        self.save(_file, alt=alt, is_overwrite=self.online)


    """
        # SEARCH

        Search related functions from metadata.

        * load_search 
            * Get a MetadataSearchHandler that automatically adds the searchable items into the data.
        
        * search_all
            * A generalized search for all of our datasets given a general term and 
    """

    def loaded_search(self, **kwargs) -> MetadataSearchHandler:
        """ 
            # LOAD SEARCH

            * Get a meatadatasearchandler that automatically adds the searchable fields into the search field.
            * Ensure to change self.searchable_items to change what we're allowed to search through
        """
        self.clear()
        current_search = self.search
        for field in self.searchable_items:
            if field in kwargs:
                current_value = kwargs.get(field)
                if current_value is not None:
                    current_search[field] = current_value
        return current_search

    def search_all(self, general: str, **kwargs):
        """ 
            # SEARCH ALL

            * Search through all of the records. 
            * To be used to find through files from the UI.
        """
        current_search = self.loaded_search(**kwargs)
        current_search.general = general
        return current_search.find()

    def file_from_dict(self, item: Dict):
        reloaded = FileEngine(
            processor=self.processor,
            name=item.name,
            category=item.category,
            subcategories=item.subcategories,
            submetatype=item.submetatype,
            abbreviation=item.abbreviation,
        )
        return reloaded
    def first(self, **kwargs) -> "FileEngine":
        """ Find a model by metadata. If there are models, get the one that exist. Otherwise return None"""
        current_search = self.loaded_search(**kwargs)

        items = current_search.find()
        if len(items) == 0:
            raise AttributeError("No model metadata found with those exact parameters")

        first = items[0]
        first = Dict(**first)

        reloaded = self.file_from_dict(first)
        reloaded.reset()

        return reloaded

    def pick(self, _id: str):
        """ Get a specific item from the system by first using the id generated"""
        self.clear()
        current_search = self.search
        item = current_search.pick(_id)
        if item is None:
            raise AttributeError("File Metadata doesn't exist")

        item = Dict(**item)
        reloaded = self.file_from_dict(item)
        reloaded.reset()
        return reloaded

    """ Reset Commands """

    def custom_post_load(self, item):
        """ Customize what happens after we load it """
        raise NotImplementedError("You need to figure out how to manage a loaded file")

    def reset_exist(self, **kwargs):
        """ Reset logic for if a file already exists """
        item = self.load_file()
        self.custom_post_load(item)
        return item

    def reset_noexist(self):
        """
            Save a file if it doesn't exist yet. Add procedure filter. Load function won't check for this.
        """
        if self.is_local_file:
            # logger.success("Starting to save file")
            if isinstance(self.blobfile, ProcedureAbstract):
                extracted = self.blobfile.extract()
                self.save_file(extracted)
                return
            self.save_file(self.blobfile)
            # logger.success("File officially saved")
        else:
            raise AttributeError(
                "You either need to have a procedure loaded or a pre-existing model"
            )

    def reset_file(self):
        """
            # RESET FILE

            Get the files. 
        """
        self.verify()

        if self.file_reset and not self.changed_since_command:
            return
        if self.is_exist_forced:
            self.reset_exist()
        else:
            self.reset_noexist()
        self.file_reset = True
        self.changed_since_command = False

    def reset(self):
        super().reset()
        if self.is_exist_forced:
            self.metaid = self.metadata.reset()
        self.reset_file()
        self.model_reset = False
        self.metrics.reset()