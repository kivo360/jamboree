import random
import time
import uuid
from typing import Optional, List

import maya
from addict import Dict
from loguru import logger

from jamboree import Jamboree
from jamboree.handlers.complex.backtestable import BacktestBlobHandler
from jamboree.handlers.complex.metric import MetricHandler
from jamboree.middleware.procedures import (
    CremeProcedure,
    ModelProcedureAbstract,
    SklearnProcedure,
    TFKerasProcedure,
    TorchProcedure,
)
from jamboree.handlers.abstracted.search.meta import MetadataSearchHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.utils.support.search import querying
from pprint import pprint


class LogInformation:
    def is_exist(self, existence):
        if existence:
            logger.success("File exist")
        else:
            logger.error("File doesn't exist")

    def open_context(self):
        logger.warning("Opening context")


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

        self.model_type = ""
        self.searchable_items = ["name", "subcategories", "abbreviation"]

        self.initialize(**kwargs)

    """ 
        Context Manager

        with compute_engine as model:
            prediction = self.model.fit_predict(data) # we'd have preprocessing strategies
    """

    def __enter__(self):
        # self.reset_model()
        self.open_context()
        return self.model

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.close_context()

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

    def init_required(self, **kwargs):
        if len(kwargs) == 0:
            return
        for k in self.required.keys():
            if k in kwargs:
                self[k] = kwargs.get(k)

    def init_specialized(self, **kwargs):
        """ Initialize all of the highly specific parts """
        logger.error("Loading specialized variables")

    """
        # Context Commands

        * Open Context - Load data and do preprocessing on it
        * Close Context - Save data, send commands, run through other customized tidbits of information
    """

    def open_context(self):
        """ Take the model from the model procedure and save it. """
        self.model.extract()

    def close_context(self):
        """ Take the model from the model procedure and save it. """
        metric_schema = {"accuracy": random.uniform(0, 1), "f1": random.uniform(0, 1)}
        logger.warning(metric_schema)
        # logger.debug(type(metric_schema))
        # self.metrics.log(metric_schema)

    """
        # Properties

        Properties that will run common for all other files/blobs.

        1. Other Handlers - For things like metrics, search and metadata
        2. Key variables - 
        3. File variables - Variables to manage the files
    """

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
        self._meta.processor = self.processor
        self._meta["name"] = self["name"]
        self._meta["category"] = self["category"]
        self._meta["subcategories"] = self["subcategories"]
        self._meta["metatype"] = self.entity
        self._meta["submetatype"] = self.model_type
        self._meta["abbreviation"] = self["abbreviation"]
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
    def available(self):
        return ["sklearn", "torch", "keras", "creme"]

    @property
    def procedure(self) -> Optional[ModelProcedureAbstract]:
        if self.custom_procedure is None:
            _customized = {
                "sklearn": SklearnProcedure(),
                "torch": TorchProcedure(),
                "keras": TFKerasProcedure(),
                "creme": CremeProcedure(),
            }.get(self.model_type)
            return _customized
            # customized
        return self.custom_procedure

    @procedure.setter
    def procedure(self, _procedure: "ModelProcedureAbstract"):
        self.custom_procedure = _procedure
        self.current_model = self.custom_procedure.extract()


    @property
    def is_procedure(self):
        return self.custom_procedure is not None

    @property
    def is_local_file(self) -> bool:
        """ Boolean explaining if we've set a file locally. Files can be set either through pulling from the DB or manually"""
        # if the blob file has been set locally, set the response to true
        return self.blobfile is not None

    @property
    def is_loaded(self) -> bool:
        """ Determines if we've loaded a model """
        return self.current_model is not None

    @property
    def model(self):
        """ 
            Returns a model procedure. 
            A procedure should be loaded on __enter__ 
        """
        return self.procedure

    def verify(self):
        """ Verifies the information that that's inside of the constructor"""
        if not isinstance(self.model_type, str) or (
            self.model_type not in self.available
        ):
            raise ValueError("Model type is not valid type")
        if not isinstance(self.online, bool):
            raise ValueError("Not able to determine if this model is online")

    @property
    def blobfile(self):
        return self.bfile

    @blobfile.setter
    def blobfile(self, _file):
        self.bfile = _file

    """
        # Database Commands

        * Save model
        * Load model
    """

    def load_model(self):
        self.current_model = self.last()
        return self.current_model

    def save_model(self, alt={}):
        self.save(self.current_model, alt=alt, is_overwrite=self.online)

    def print_latest_result(self):
        proving_its_there = self.metadata.search.find()
        logger.info(proving_its_there)

        # No idea what else should be here

    

    """
        Reset Commands
    """

    # def reset_model(self):
    #     """ Reset the model to be used """

    #     self.verify()

    #     # NOTE: Time to split this logic to prevent errors
    #     if self.model_reset or self.has_changed:
    #         return

    #     if self.is_exist:
    #         self.reset_exist()
    #     else:
    #         self.reset_noexist()

    #     self.model_reset = True

    

    """
        # Search

        All search related functions.
    """

    def loaded_search(self, **kwargs) -> MetadataSearchHandler:
        self.clear()
        current_search = self.search
        for field in self.searchable_items:
            if field in kwargs:
                current_value = kwargs.get(field)
                if current_value is not None:
                    current_search[field] = current_value
        return current_search

    def first(self, **kwargs):
        """ Find a model by metadata. If there are models, get the one that exist. Otherwise return None"""
        current_search = self.loaded_search(**kwargs)

        items = current_search.find()
        if len(items) == 0:
            raise AttributeError("No model metadata found with those exact parameters")

        first = items[0]
        logger.error(first)
        model_type = first.get("submetatype")
        reloaded = FileEngine()
        reloaded.model_type = model_type
        reloaded["name"] = first.get("name")
        reloaded["category"] = first.get("category")
        reloaded["subcategories"] = first.get("subcategories")
        reloaded["submetatype"] = model_type
        reloaded["abbreviation"] = first.get("abbreviation")
        reloaded.processor = self.processor
        reloaded.changed_since_command = True
        reloaded.reset()
        return reloaded

    def search_all(self, general: str, **kwargs):
        current_search = self.loaded_search(**kwargs)
        current_search.general = general
        return current_search.find()

    def pick(self, _id: str):
        """ Get a specific item from the system by first using the id generated"""
        self.clear()
        current_search = self.search
        item = current_search.pick(_id)
        if item is None:
            raise AttributeError("File Metadata doesn't exist")

        # Create a load function you can pull the data from the dictionary from
        model_type = item.get("submetatype")
        reloaded = FileEngine()
        reloaded.model_type = model_type
        reloaded["name"] = item.get("name")
        reloaded["category"] = item.get("category")
        reloaded["subcategories"] = item.get("subcategories")
        reloaded["submetatype"] = model_type
        reloaded["abbreviation"] = item.get("abbreviation")
        reloaded.processor = self.processor
        reloaded.reset()
        logger.info(reloaded)
        return reloaded



    """ Reset Commands """

    def reset_exist(self):
        """
            Splitting the logic for things that do exist
        """
        model = self.load_model()
        proc = self.procedure
        # proc
        proc.mdict = model
        proc.verify()
        self.procedure = proc


    def reset_noexist(self):
        """
            Splitting the logic for things that don't exist
        """
        if self.is_local_file:
            logger.success("Detecting file locally!!!")
            logger.info("Preprocessing the file and doing the appropiate checks")
            logger.success("Saving the file")
        else:
            raise AttributeError(
                "You either need to have a procedure loaded or a pre-existing model"
            )


    def reset_file(self):
        # Run through the custom verification scheme here
        self.verify()
        if self.is_exist:
            logger.success("File exist")
        else:
            self.reset_noexist()
        self.file_reset = True


    def reset(self):
        super().reset()
        self.reset_file()
        if self.is_exist_forced:
            self.metadata.reset()
        # self.model_reset = False
        # self.reset_model()
        # # self.metrics.reset()


class ModelEngine(FileEngine):
    """ """

    def __init__(self, processor, **kwargs):
        super().__init__(processor=processor, **kwargs)

    # def reset_exist(self):
    #     """
    #         Splitting the logic for things that do exist
    #     """
    #     model = self.load_model()
    #     proc = self.procedure
    #     # proc
    #     proc.mdict = model
    #     proc.verify()
    #     self.procedure = proc


    # def reset_noexist(self):
    #     """
    #         Splitting the logic for things that don't exist
    #     """
    #     if self.is_local_file:
    #         # proc = self.procedure
    #         # proc.verify()
    #         # self.current_model = proc.extract()
    #         # self.save_model()
    #         # self.metadata.search.reset()
    #     else:
    #         raise AttributeError(
    #             "You either need to have a procedure loaded or a pre-existing model"
    #         )

    # def reset_exist(self):
    #     """
    #         Splitting the logic for things that do exist
    #     """
    #     model = self.load_model()
    #     proc = self.procedure
    #     # proc
    #     proc.mdict = model
    #     proc.verify()
    #     self.procedure = proc

    # def init_specialized(self, **kwargs):
    #     """ Initialize all of the highly specific parts """
    #     self.net = None
    #     self.criterion = None
    #     self.optimizer = None

    #     self.current_model = None
    #     self.custom_procedure: Optional[ModelProcedureAbstract] = None
    #     self.model_reset = False


class GenericClass(object):
    def __init__(self):
        self.x = 0
        self.y = 0



def main():

    """
        # Model Engine Use Case
        

        ### Custom Torch Handler Should Have A Model Inside Already
        compute_engine = CustomTorchHandler()
        compute_engine.processor = jam
        compute_engine['name'] = "MNIST
        compute_engine['category'] = "image"
        compute_engine['subcategories'] = {}
        compute_engine.reset()

        with compute_engine as model:
            prediction = model.fit_predict(data)

        Use prediction here
    """

    from jamboree.middleware.procedures.models.learn import (
        CustomSklearnGaussianProcedure,
    )

    file_name = uuid.uuid4().hex
    logger.debug("Starting model experiment")
    jamboree_processor = Jamboree()
    compute_engine = FileEngine()
    compute_engine.model_type = "sklearn"
    compute_engine.processor = jamboree_processor
    compute_engine.online = True
    compute_engine.procedure = CustomSklearnGaussianProcedure()
    compute_engine["name"] = file_name
    compute_engine["submetatype"] = "sklearn"
    compute_engine["abbreviation"] = "GAUSSLEARN"

    compute_engine["category"] = "imaging"
    compute_engine["subcategories"] = {"ml_type": "gaussian"}
    compute_engine.time.change_stepsize(microseconds=0, seconds=0, hours=4)

    compute_engine.reset()

    with compute_engine as model:
        logger.info(model.extract())

    for _ in range(100):
        with compute_engine as model:
            logger.info(model)
        compute_engine.time.step()

    reloaded = compute_engine.first(name=file_name)

    all_models = compute_engine.search_all("image")
    compute_engine.pick("33a3eb94a41c4d9ebc453b90b1986c0f")
    logger.success(all_models)
    with reloaded as model:
        logger.debug(f"The Procedure we're using is ... {dir(model)}")
    


def file_engine_main():
    """ 
        Creating a generic usage of the file engine instead of only model storage. 

        To test, we're going to entirely duplicate the prior test. 
        Only we're going to use generic functions and variables. In essensce, rebuild the `ModelEngine` starting with the file handler

        Key steps:

        1. Create a subclass of the file engine for the model engine
        2. Set a file
        3. 
    """

    from jamboree.middleware.procedures.models.learn import (
        CustomSklearnGaussianProcedure,
    )

    file_name = uuid.uuid4().hex
    logger.info("Starting file engine experiment")
    logger.info(f"The file name is: {file_name}")
    jamboree_processor = Jamboree()
    with logger.catch(message="This should fail immediately"):
        # Initialize a file engine
        model_engine = ModelEngine(
            processor=jamboree_processor,
            name=file_name,
            category="machine",
            subcategories={"ml_type": "gaussian"},
            abbreviation="GAUSS",
            submetatype="sklearn",
            blobfile=CustomSklearnGaussianProcedure(),
        )
        model_engine.reset()


if __name__ == "__main__":
    file_engine_main()
