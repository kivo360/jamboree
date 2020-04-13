from addict import Dict
from loguru import logger
from sklearn.base import BaseEstimator
from sklearn.datasets import make_friedman2
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import DotProduct, WhiteKernel
from jamboree.middleware.procedures import ModelProcedureAbstract



class SklearnProcedure(ModelProcedureAbstract):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.requirements.model = True
        self.requirements.criterion = False
        self.requirements.optimizer = False
        self.types.model = BaseEstimator
        self.changed = False
    


    @property
    def model(self) -> BaseEstimator:
        self.verify()
        return self.dictionary.model
        
    def set_params(self, **params):
        self.changed = True
        self.model.set_params(**params)

    @logger.catch
    def get_params(self):
        return self.model.get_params()
        
    @logger.catch
    def predict(self, X, **kwargs):
        return self.model.predict(X, **kwargs)
    
    @logger.catch
    def predict_proba(self, X, **kwargs):
        prediction = self.model.predict_proba(X, **kwargs)
        return prediction

    @logger.catch
    def partial_fit(self, X, y, **kwargs):
        self.changed = True
        self.model.partial_fit(X, y, **kwargs)

    def fit(self, X, y, **kwargs):
        self.changed = True
        self.model.fit(X, y, **kwargs)

class CustomSklearnGaussianProcedure(SklearnProcedure):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        kernel = DotProduct() + WhiteKernel()
        gpr = GaussianProcessRegressor(kernel=kernel, random_state=0)
        self.dictionary.model = gpr


if __name__ == "__main__":
    general_procedure = CustomSklearnGaussianProcedure()
    X, y = make_friedman2(n_samples=500, noise=0, random_state=0)
    general_procedure.fit(X, y)
    print(general_procedure.predict(X[:2,:], return_std=True))
    # print(general_procedure.get_params())
    print(general_procedure.extract())

