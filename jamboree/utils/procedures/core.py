from abc import ABC
from addict import Dict as ADict


class ProcedureAbstract(ABC):
    """ 
        Procedures ensure everything is consistent.
    """

    def verify(self):
        """ Ensures we have all of the required variables in place."""
        raise NotImplementedError




class ModelProcedureAbstract(ProcedureAbstract):
    def __init__(self):
        self._mod = None
        self._opt = None
        self._crit = None
        
        self._model_dict = ADict()
        self._model_dict.model = None
        self._model_dict.optimizer = None
        self._model_dict.criteria = None

        self._model_typing = ADict()
        self._model_typing.model = None
        self._model_typing.optimizer = None
        self._model_typing.criteria = None

        self._model_requirements = ADict()
        self._model_requirements.model = True
        self._model_requirements.optimizer = False
        self._model_requirements.criteria = False
        

    @property
    def mdict(self):
        return self._model_dict
    
    @mdict.setter
    def mdict(self, _md:ADict):
        """ Load in raw model dict information """
        self._model_dict.update(_md)

    @property
    def mreqs(self) -> ADict:
        return self._model_requirements
    
    @mreqs.setter
    def mreqs(self, _md:ADict):
        """ Load in raw model dict information """
        self._model_requirements.update(_md)
    
    @property
    def mtypes(self) -> ADict:
        return self._model_typing

    @mtypes.setter
    def mtypes(self, _mt:ADict):
        self._model_typing.update(_mt)

    """
        Verification
    """

    def verify_model_typing(self):
        """Check that none of the model types are none """
        for k, v in self.mreqs.items():
            if not isinstance(v, bool):
                raise ValueError(f"Model Requirement \'{k}\' must be a boolean value")
            if v == True:
                if self.mtypes[k] is None:
                    raise ValueError(f"\'{k}\' Cannot be None in typing delarations")
                if self.mdict[k] is None:
                    raise ValueError(f"\'{k}\' Cannot be None inside of the main model dictionary")
    
    def verify_model_dict(self):
        """ Verify that """
        for name, _type in self.mtypes.items():
            if name is None or _type is None:
                continue
            if not isinstance(self.mdict[name], _type):
                raise TypeError(f"{name} is not an instance of {_type}")

    def verify(self):
        self.verify_model_typing()
        self.verify_model_dict()
    
    def is_valid_data(self, _data) -> bool:
        """ Determines if the data we're about to use is valid"""
        raise NotImplementedError("Data validation not implemented yet")

    def split(self, X, y, **params):
        raise NotImplementedError

    def fit(self, X, y, **params):
        raise NotImplementedError

    def partial_fit(self, X, y, **params):
        raise NotImplementedError
    
    def predict(self, X, **params):
        raise NotImplementedError

    def predict_prob(self, X, **params):
        raise NotImplementedError

    def score(self, X, y, **params):
        raise NotImplementedError

    def get_params(self, **params):
        raise NotImplementedError

    def set_params(self, **params):
        raise NotImplementedError

    def extract(self):
        """ Get a dictionary to save the model. Should be called in close """
        return self.mdict

    @property
    def metrics(self):
        """ Given the information we have, return a set of metrics"""
        metric_set = {}
        
        return metric_set

    def add_metric(self, _metric):
        pass

if __name__ == "__main__":
    model_types = ADict()
    model_vals = ADict()
    model_types.model = bool
    model_types.optimizer = str
    model_types.criteria = str

    model_vals.model = False
    model_vals.optimizer = "str"
    model_vals.criteria = "str"

    base_model_procedure = ModelProcedureAbstract()
    base_model_procedure.mtypes = model_types
    base_model_procedure.mdict = model_vals
    base_model_procedure.verify()
    
    print(base_model_procedure)