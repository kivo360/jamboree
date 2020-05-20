from abc import ABC
from typing import Any


class DataProcessorsAbstract(ABC):
    """ DataProcessor is used to transform"""
    def __init__(self, name, **kwargs):
        self._name = name
        self.set_settings(**kwargs)

    def set_settings(self, **kwargs):
        raise NotImplementedError(
            "Need to set the settings you're expecting for this preprocessor"
        )
    
    def process(self, data:Any) -> Any:
        raise NotImplementedError(
            "A command to preprocess information and return that info."
        )

