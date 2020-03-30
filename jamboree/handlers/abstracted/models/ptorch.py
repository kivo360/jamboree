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
from jamboree.handlers.complex.model import BaseModelHandler

from jamboree.middleware.procedures.models.learn import CustomSklearnGaussianProcedure
from jamboree.middleware.procedures.models.ptorch import TestCustomTorchClassifier
from jamboree.middleware.procedures import (
    CremeProcedure,
    ModelProcedureAbstract,
    SklearnProcedure, TFKerasProcedure,
    TorchProcedure
)


class PytorchModelHandler(BaseModelHandler):
    def __init__(self, *args, **kargs):
        super().__init__()
        self.model_type = "torch"


class PyTorchClassifierHandler(PytorchModelHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['name'] = "GAUSSIAN"
        self['category'] = "regression"
        self['subcategories'] = {
            "ml_type": "ml_model_goes_here"
        }
        self.procedure = TestCustomTorchClassifier()
        self.online = True