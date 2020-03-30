import random
import time
import uuid
from typing import Optional

import maya
from addict import Dict
from loguru import logger

from jamboree import Jamboree
from jamboree.handlers.complex.backtest import BacktestBlobHandler
from jamboree.handlers.complex.metric import MetricHandler
from jamboree.middleware.procedures import (
    CremeProcedure,
    ModelProcedureAbstract,
    SklearnProcedure, TFKerasProcedure,
    TorchProcedure
)


class LogInformation:

    def is_exist(self, existence):
        if existence:
            logger.success("File exist")
        else:
            logger.error("File doesn't exist")
    
    def open_context(self):
        logger.warning("Opening context")




class BaseModelHandler(BacktestBlobHandler):
    def __init__(self) -> None:
        super(BaseModelHandler, self).__init__()
        self.entity = "genmodel" # Gen model represents general model
        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict # other information regarding the library type.  
        }
        self.model_type = ""
        
        self._metrics: MetricHandler = MetricHandler()


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
    
    def close_context(self):
        """ Take the model from the model procedure and save it. """
        if self.is_logging:
            logger.debug("Closing the model here")
            logger.debug("Saving metrics about the model")
        self.current_model = self.model.extract()
        self.save_model()
        self.metrics.reset_current_metric()
        metric_schema = {
            "accuracy": random.uniform(0, 1),
            "f1": random.uniform(0, 1)
        }
        self.metrics.log(metric_schema)

    @property
    def available(self):
        return [
            "sklearn", "torch", "keras", "creme"
        ]
    
    @property
    def procedure(self) -> Optional[ModelProcedureAbstract]:
        if self.custom_procedure is None:
            return {
                "sklearn":SklearnProcedure, 
                "torch":TorchProcedure, 
                "keras":TFKerasProcedure, 
                "creme":CremeProcedure
            }.get(self.model_type)
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
        return self._metrics


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


    



    """
        Reset Commands
    """

    def reset_model(self):
        """ Reset the model to be used """

        self.verify()
        if self.model_reset or self.is_key_change:
            return
        
        if self.is_logging:
            logger.warning("Resetting the model")
        if ((not self.is_exist) and (self.is_procedure)):
            """ 
                Initiation Conditions:
                    - if the model doesn't exist in the database
                    - if there is an existing procedure item
                    - Get the model from the procedure
                    - Store it to the database
            """
            self.current_model = self.procedure.extract()
            self.save_model()
        elif self.is_exist and (not self.is_procedure):
            self.load_model()
            self.procedure.mdict = self.current_model
        elif (not self.is_exist) and (not self.is_procedure):
            """ 
                Initiation Conditions:
                    - if the model doesn't exist in the database
                    - if there is no procedure in the object
                    - if load directly is
                    - load directly from the bundled set
            """
            _bundled = self.bundled
            self.procedure.mdict = _bundled
            # It'll crash if it doesn't fit the information accordingly
            self.current_model = _bundled
        
        self.model_reset = True
        
        # bvals = self.bundled.values()
        

    def reset(self):
        super().reset()

        self.model_reset = False
        self.reset_model()
        self.metrics.reset()





def main():
    from jamboree.middleware.procedures.models.learn import CustomSklearnGaussianProcedure
    jamboree_processor = Jamboree()
    compute_engine = BaseModelHandler()
    # compute_engine.is_logging = True
    compute_engine.model_type = "sklearn"
    compute_engine.processor = jamboree_processor
    compute_engine.online = True
    compute_engine.procedure = CustomSklearnGaussianProcedure()
    compute_engine['name'] = "MNIST"
    compute_engine['category'] = "imagingg"
    compute_engine['subcategories'] = {
        "ml_type": "ml_model_goes_here"
    }
    compute_engine.time.change_stepsize(microseconds=0, seconds=0, hours=4)
    compute_engine.reset()

    with compute_engine as model:
        logger.info(model)
    
    for _ in range(100):
        with compute_engine as model:
            logger.info(model)
        compute_engine.time.step()

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
