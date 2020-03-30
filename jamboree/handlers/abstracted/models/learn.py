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
from jamboree.middleware.procedures import (
    CremeProcedure,
    ModelProcedureAbstract,
    SklearnProcedure, TFKerasProcedure,
    TorchProcedure
)


class SkLearnModelHandler(BaseModelHandler):
    def __init__(self, *args, **kargs):
        super().__init__()
        self.model_type = "sklearn"


class SklearnGaussianHandler(SkLearnModelHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['name'] = "GAUSSIAN"
        self['category'] = "regression"
        self['subcategories'] = {
            "ml_type": "ml_model_goes_here"
        }
        self.procedure = CustomSklearnGaussianProcedure()
        self.online = True
    
    


jamboree_processor = Jamboree()
compute_engine = SklearnGaussianHandler()
compute_engine.processor = jamboree_processor
compute_engine.time.change_stepsize(microseconds=0, seconds=0, hours=4)
compute_engine.reset()



if __name__ == "__main__":
    with compute_engine as model:
        logger.info(model)

    for _ in range(100):
        with compute_engine as model:
            logger.info(model)
        compute_engine.time.step()