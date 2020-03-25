import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.complex.backtest import BacktestBlobHandler
from jamboree import Jamboree
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
    
    def verify(self):
        """ Verifies the information that that's inside of the constructor"""
        if not isinstance(self.model_type, str) or (self.model_type not in self.available):
            raise ValueError(f"Model type is not valid type: [{self.available}]")
        if not isinstance(self.online, bool):
            raise ValueError("The online variable needs to be a bool")

    @property
    def check_bundle(self):
        """ Checks to see if we have necessary parts for the model. If we do, it gets a pass and doesn't raise an error"""
        for req_name, is_req in self._mreq.items():
            if is_req:
                if self._mparts.get(req_name, None) is None:
                    raise AttributeError(f"\'{req_name}\' requirement not found")
        return True

    @property
    def criterion(self):
        return self._mparts.get("criterion", None)
    
    @criterion.setter
    def criterion(self, _criterion):
        self._mparts['criterion'] = _criterion

    @property
    def optimizer(self):
        return self._mparts.get("optimizer", None)
    
    @optimizer.setter
    def optimizer(self, _opt):
        self._mparts['optimizer'] = _opt

    @property
    def model(self):
        return self._mparts.get("model", None)

    @model.setter
    def model(self, _model):
        self._mparts['model'] = _model
    
    @property
    def bundled(self):
        """ A bundled model. All of the model in a dictionary """
        self.check_bundle
        return self._mparts
    
    def save_model(self, alt={}):
        full_model = self.bundled
        self.save(full_model, alt=alt, is_overwrite=self.online)

    def load_model(self, alt={}):
        """ Load the latest model by query parameters"""
        # self.
        self.model = self.last(alt=alt)
        print(self.model)
    
    """
        Separate the processors
    """

    def split(self, X, y):
        """ Get a train test split. Fully determined by the actual ML model"""
        pass

    def score(self, X, y):
        """ Allows us to """

    def log_metrics(self):
        pass
    
    def fit_predict(self, X, y):
        self.load_model()
    
    def partial_fit_predict(self, X, y):
        self.load_model()

    def partial_fit(self, X, y):
        self.load_model()

    def fit(self, X, y):
        self.load_model()

    def predict(self, X):
        self.load_model()

    def reset(self):
        # logger.success("Starting criteria check")
        super().reset()

class TorchModelHandler(ModelHandler):
    pass


def main():
    jam = Jamboree()
    mhand = ModelHandler()
    mhand['name'] = "online_regression"
    mhand['category'] = "market"
    mhand['subcategories'] = {}
    mhand.processor = jam
    mhand.reset()
    mhand.online = True
    # mhand.model = "DICKS"
    # mhand.save_model()
    # mhand.fit(1, 2)



if __name__ == "__main__":
    main()