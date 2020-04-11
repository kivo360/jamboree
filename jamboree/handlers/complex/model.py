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
    SklearnProcedure, TFKerasProcedure,
    TorchProcedure
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




class BaseModelHandler(BacktestBlobHandler):
    _instance = None
    def __init__(self) -> None:
        super(BaseModelHandler, self).__init__()
        self.entity = "models" # Gen model represents general model
        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict, # other information regarding the library type.  
            # Search specific information
            "metatype": str,
            "submetatype":str,
            "abbreviation": str,
        }
        self.model_type = ""
        self.searchable_items = ["name", "subcategories", "abbreviation"]
        self._metrics: MetricHandler = MetricHandler()
        # Use to find price information
        self._metasearch:MetadataSearchHandler = MetadataSearchHandler()
        self._meta: MetaHandler = MetaHandler()

        # All of the parts of the model we'll
        self.net = None
        self.criterion = None
        self.optimizer = None

        self.online = False
        self.current_model = None
        self.custom_procedure:Optional[ModelProcedureAbstract] = None
        self.model_reset = False
        self.is_logging = False
        self.log = LogInformation()
        self['metatype'] = self.entity


    """ 
        Context Manager

        with compute_engine as model:
            prediction = self.model.fit_predict(data) # we'd have preprocessing strategies
        
        print(prediction)
    """
    

    def __enter__(self):
        self.reset_model()
        self.open_context()
        return self.model

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.close_context()

    """
        # Context Commands

        * Open Context - Do everything 
    """


    def open_context(self):
        """ Take the model from the model procedure and save it. """
        if self.is_logging:
            logger.success("Opening the model here")
            logger.success("Prepare for model use")
        self.model.extract()
    
    def close_context(self):
        """ Take the model from the model procedure and save it. """
        if self.is_logging:
            logger.debug("Closing the model here")
            logger.debug("Saving metrics about the model")
        # print(self.model.extract())
        # self.current_model = self.model.extract()
        # self.save_model()
        # self.metrics.reset_current_metric()
        metric_schema = {
            "accuracy": random.uniform(0, 1),
            "f1": random.uniform(0, 1)
        }
        logger.warning(metric_schema)
        # logger.debug(type(metric_schema))
        self.metrics.log(metric_schema)

    @property
    def available(self):
        return [
            "sklearn", "torch", "keras", "creme"
        ]
    
    @property
    def procedure(self) -> Optional[ModelProcedureAbstract]:
        if self.custom_procedure is None:
            _customized = {
                "sklearn":SklearnProcedure(), 
                "torch":TorchProcedure(), 
                "keras":TFKerasProcedure(), 
                "creme":CremeProcedure()
            }.get(self.model_type)
            return _customized
            # customized
        return self.custom_procedure
    
    @procedure.setter
    def procedure(self, _procedure:'ModelProcedureAbstract'):
        self.custom_procedure = _procedure
        self.current_model = self.custom_procedure.extract()
    
    @property
    def is_key_change(self) -> bool:
        return self.changed_since_command
            # return
        # self.procedure self.current_model


    @property
    def bundled(self):
        return {
                "model": self.net,
                "optimizer": self.optimizer,
                "criterion": self.criterion
            }

    @property
    def is_exist(self) -> bool:
        """ Checks to see if a model exist in storage."""
        existence = self.absolute_exists()
        if self.is_logging:
            self.log.is_exist(existence)
        return existence
    
    @property
    def is_procedure(self):
        return self.custom_procedure is not None


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

    @property    
    def metrics(self):
        self._metrics.processor = self.processor
        self._metrics['name'] = self["name"]
        self._metrics['category'] = self["category"]
        self._metrics['subcategories'] = self["subcategories"]
        self._metrics.episode = self.episode
        self._metrics.live = self.live
        self._metrics.time = self.time
        self._metrics.reset()
        return self._metrics
    
    @property
    def metadata(self):
        self._meta.processor = self.processor
        self._meta['name'] = self['name']
        self._meta['category'] = self['category']
        self._meta['subcategories'] = self['subcategories']
        self._meta['metatype'] = self.entity
        self._meta['submetatype'] = self.model_type
        self._meta['abbreviation'] = self['abbreviation']
        return self._meta

    @property
    def search(self):
        self._metasearch.reset()
        self._metasearch['category'] = querying.text.exact(self['category'])
        self._metasearch['metatype'] = querying.text.exact(self.entity)
        self._metasearch['submetatype'] = querying.text.exact(self['submetatype'])
        self._metasearch.processor = self.processor
        return self._metasearch


    def verify(self):
        """ Verifies the information that that's inside of the constructor"""
        if not isinstance(self.model_type, str) or (self.model_type not in self.available):
            raise ValueError("Model type is not valid type")
        if not isinstance(self.online, bool):
            raise ValueError("Not able to determine if this model is online")
    

    """
        # Database Commands

        * Save model
        * Load model
    """

    def load_model(self):
        logger.debug("Loading Model")
        self.current_model = self.last()
        return self.current_model

    def save_model(self, alt={}):
        if self.is_logging:
            logger.debug("Saving model")
        self.save(self.current_model, alt=alt, is_overwrite=self.online)

    
        # No idea what else should be here



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
        if self.is_procedure:
            proc = self.procedure
            proc.verify()
            self.current_model = proc.extract()
            self.save_model()
        else:
            raise AttributeError("You either need to have a procedure loaded or a pre-existing model")


    """
        Reset Commands
    """

    def reset_model(self):
        """ Reset the model to be used """

        self.verify()

        # NOTE: Time to split this logic to prevent errors
        if self.model_reset or self.is_key_change:
            return
        
        if self.is_logging:
            logger.warning("Resetting the model")
        
        if self.is_exist:
            self.reset_exist()
        else:
            self.reset_noexist()
        
        self.model_reset = True
    
    
    def first(self, **kwargs):
        """ Find a model by metadata. If there are models, get the one that exist. Otherwise return None"""
        current_search = self.search
        name = kwargs.get("name", None)
        subcategories = kwargs.get("subcategories", None)
        abbv = kwargs.get("abbreviation", None)
        if name is not None:
            current_search['name'] = name
        if subcategories is not None:
            current_search['subcategories'] = subcategories
        if abbv is not None:
            current_search['abbreviation'] = abbv
        items = current_search.find()
        if len(items) == 0:
            return None

        first = items[0]
        model_type = first.get("submetatype")
        reloaded = BaseModelHandler()
        reloaded.model_type = model_type
        reloaded['name'] = first.get("name")
        reloaded['category'] = first.get("category")
        reloaded['subcategories'] = first.get("subcategories")
        reloaded['submetatype'] = model_type
        reloaded['abbreviation'] = first.get("abbreviation")
        reloaded.processor = self.processor
        reloaded.reset()
        return reloaded
    
    def search_all(self, general_terms, **kwargs):
        current_search = self.search
        name =          kwargs.get("name", None)
        subcategories = kwargs.get("subcategories", None)
        abbv =          kwargs.get("abbreviation", None)
        if name is not None:
            current_search['name'] = name
        if subcategories is not None:
            current_search['subcategories'] = subcategories
        if abbv is not None:
            current_search['abbreviation'] = abbv
        
        return current_search.find()
    

    def pick(self, _id:str):
        
        pass

        

    def reset(self):
        super().reset()

        self.model_reset = False
        self.reset_model()
        # self.metrics.reset()
        self.metadata.reset()





def main():
    from jamboree.middleware.procedures.models.learn import CustomSklearnGaussianProcedure
    logger.debug("Starting model experiment")
    jamboree_processor = Jamboree()
    compute_engine = BaseModelHandler()
    # compute_engine.is_logging = True
    compute_engine.model_type = "sklearn"
    compute_engine.processor = jamboree_processor
    compute_engine.online = True
    compute_engine.procedure = CustomSklearnGaussianProcedure()
    compute_engine['name'] = "MNIST"
    compute_engine['submetatype'] = "sklearn"
    compute_engine['abbreviation'] = "GAUSSLEARN"
    
    compute_engine['category'] = "imaging"
    compute_engine['subcategories'] = {
        "ml_type": "gaussian"
    }
    # compute_engine.time.change_stepsize(microseconds=0, seconds=0, hours=4)
    # compute_engine.reset()

    # with compute_engine as model:
    #     logger.info(model.extract())
    
    # for _ in range(100):
    #     with compute_engine as model:
    #         logger.info(model)
    #     compute_engine.time.step()

    reloaded = compute_engine.first(name="MNIST")
    
    all_models = compute_engine.search_all("image")
    logger.success(all_models)

    with reloaded as model:
        logger.debug(f"The Procedure we're using is ... {model}")
    """
        # EXAMPLE USE CASE
        

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



if __name__ == "__main__":
    main()
