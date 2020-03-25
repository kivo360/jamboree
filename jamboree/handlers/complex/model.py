import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.complex.backtest import BacktestBlobHandler
from jamboree import Jamboree
# from jamboree.procedures import ModelProcedureAbstract
from addict import Dict

class ModelHandler(BacktestBlobHandler):
    def __init__(self) -> None:
        super(ModelHandler, self).__init__()
        self.entity = "genmodel" # Gen model represents general model
        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict # other information regarding the library type.  
        }
        self.model_type = ""
        # 
        # All of the parts of the model we'll
        self._mparts = {
            "criterion": None,
            "optimizer": None,
            "model": None
        }

        self._mreq = {
            "criterion": False,
            "optimizer": False,
            "model": True
        }
        self.online = False
        self.current_model = None

    @property
    def available(self):
        return [
            "sklearn", "torch", "keras", "creme", "tensorflow", "sonnet", "jax"
        ]
    
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
        compute_engine = CustomTorchHandler()

    """
    # jam = Jamboree()
    # mhand = ModelHandler()
    # mhand['name'] = "online_regression"
    # mhand['category'] = "market"
    # mhand['subcategories'] = {}
    # mhand.processor = jam
    # mhand.reset()
    # mhand.online = True
    # # mhand.model = "DICKS"
    # # mhand.save_model()
    # # mhand.fit(1, 2)



if __name__ == "__main__":
    main()