from addict import Dict
from sklearn.base import BaseEstimator
from jamboree.middleware.procedures import ModelProcedureAbstract
from sklearn.datasets import make_friedman2
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import DotProduct, WhiteKernel
from loguru import logger


""" TODO: FIX THIS CRAP!!!"""


class TFKerasProcedure(ModelProcedureAbstract):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.requirements.model = True
        self.requirements.criterion = False
        self.requirements.optimizer = False
        
        # types = Dict()
        # types.model = BaseEstimator
        
        self.types.model = BaseEstimator
    
    @logger.catch
    def get_params(self):
        self.verify()
        return self.dictionary.model.get_params()
        
    @logger.catch
    def predict(self, X, **kwargs):
        self.verify()
        return self.dictionary.model.predict(X, **kwargs)
    
    @logger.catch
    def predict_prob(self, X, **kwargs):
        self.verify()
        return self.dictionary.model.predict_prob(X, **kwargs)

    @logger.catch
    def partial_fit(self, X, y, **kwargs):
        self.verify()
        self.dictionary.model.partial_fit(X, y, **kwargs)

    def fit(self, X, y, **kwargs):
        self.verify()
        self.dictionary.model.fit(X, y, **kwargs)
        # print(self.mdict.model.predict(X[:2,:], return_std=True))