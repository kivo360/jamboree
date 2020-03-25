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
    def modict(self):
        return self._model_dict
    
    @modict.setter
    def modict(self, _md:ADict):
        """ Load in raw model dict information """
        self._model_dict.update(_md)

    @property
    def modreq(self) -> dict:
        return self._model_requirements
    
    @modreq.setter
    def modreq(self, _md:ADict):
        """ Load in raw model dict information """
        self._model_requirements.update(_md)
    
    @property
    def motype(self) -> dict:
        return self._model_typing

    @motype.setter
    def motype(self, _mt:ADict):
        self._model_typing.update(_mt)

    """
        Verification
    """

    def verify_model_typing(self):
        """Check that none of the model types are none """
        for k, v in self.modreq.items():
            if not isinstance(v, bool):
                raise ValueError(f"Model Requirement \'{k}\' must be a boolean value")
            if v == True:
                if self.motype[k] is None:
                    raise ValueError(f"\'{v}\' Can not be None in typing delarations")
                if self.modict[k] is None:
                    raise ValueError(f"\'{v}\' Can not be None inside of the main model dictionary")
    
    def verify_model_dict(self):
        """ Verify that """
        for name, _type in self.motype.items():
            print(name)
            print(_type)

    def verify(self):
        self.verify_model_typing()
        self.verify_model_dict()


if __name__ == "__main__":
    base_model_procedure = ModelProcedureAbstract()
    print(base_model_procedure)