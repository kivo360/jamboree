import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.complex.backtest import BacktestBlobHandler
from jamboree import Jamboree
from jamboree.utils.procedures import ModelProcedureAbstract
from addict import Dict

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
        # 
        # All of the parts of the model we'll
        self.net = None
        self.criterion = None
        self.optimizer = None

        self.online = False
        self.current_model = None

    @property
    def available(self):
        return [
            "sklearn", "torch", "keras", "creme", "tensorflow", "sonnet", "jax"
        ]
    
    @property
    def procedure(self):
        return {

        }[self.model_type]
    
    def open(self):
        if self.absolute_exists():
            self.current_model = self.last()

    def close(self):
        """ Take the model from the model procedure and save it. """
        pass

    @property
    def model(self):
        if self.current_model is None:
            """ Create a model using the net, criterion, and optimizer items """
            mdict = {
                "model": self.net,
                "optimizer": self.optimizer,
                "criterion": self.criterion
            }
            pass
        pass

    def verify(self):
        """ Verifies the information that that's inside of the constructor"""
        if not isinstance(self.model_type, str) or (self.model_type not in self.available):
            raise ValueError("Model type is not valid type")
        if not isinstance(self.online, bool):
            raise ValueError("Not able to determine if this model is online")
        # No idea what else should be here

    """
        Separate the processors
    """

    def reset(self):
        # logger.success("Starting criteria check")
        super().reset()



def main():
    """
        # EXAMPLE USE CASE
        jam = Jamboree()

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