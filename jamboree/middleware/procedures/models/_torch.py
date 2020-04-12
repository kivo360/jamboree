from addict import Dict
from loguru import logger
from typing import Optional

from sklearn.base import BaseEstimator
from sklearn.datasets import make_friedman2
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import DotProduct, WhiteKernel
from jamboree.middleware.procedures import ModelProcedureAbstract

from skorch.net import NeuralNet

from torch.nn import Module
from torch.nn.modules.loss import _Loss
from torch.optim import Optimizer
from torch.optim import Adam
import numpy as np
from sklearn.datasets import make_classification
from torch import nn
import torch.nn.functional as F

from skorch import NeuralNetClassifier


class TorchProcedure(ModelProcedureAbstract):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.requirements.model = True
        self.requirements.criterion = True
        self.requirements.optimizer = True
        self.types.model = nn.Module
        self.types.criterion = _Loss
        self.types.optimizer = Optimizer
        self._compiled_model:Optional[NeuralNet] = None
    
    @property
    def model(self) -> NeuralNet:
        self.verify()
        if self._compiled_model is None:
            _compiled = NeuralNet(
                module=self.dictionary.model,
                criterion=self.dictionary.criterion,
                optimizer=self.dictionary.optimizer,
                max_epochs=10,
                lr=0.1,
                # Shuffle training data on each epoch
                iterator_train__shuffle=True,
            )
            self._compiled_model = _compiled
        return self._compiled_model

    @logger.catch
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
        return self.model.predict_proba(X, **kwargs)

    @logger.catch
    def partial_fit(self, X, y, **kwargs):
        self.changed = True
        self.model.partial_fit(X, y, **kwargs)

    def fit(self, X, y, **kwargs):
        self.changed = True
        self.model.fit(X, y, **kwargs)


class MyModule(nn.Module):
    def __init__(self, num_units=10, nonlin=F.relu):
        super(MyModule, self).__init__()

        self.dense0 = nn.Linear(20, num_units)
        self.nonlin = nonlin
        self.dropout = nn.Dropout(0.5)
        self.dense1 = nn.Linear(num_units, 10)
        self.output = nn.Linear(10, 2)

    def forward(self, X, **kwargs):
        X = self.nonlin(self.dense0(X))
        X = self.dropout(X)
        X = F.relu(self.dense1(X))
        X = F.softmax(self.output(X))
        return X


class TestCustomTorchClassifier(TorchProcedure):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dictionary.model = MyModule
        self.dictionary.optimizer = Adam
        self.dictionary.criterion = nn.NLLLoss




def main():
    


    X, y = make_classification(1000, 20, n_informative=10, random_state=0)
    X = X.astype(np.float32)
    y = y.astype(np.int64)


    net = TestCustomTorchClassifier()

    net.fit(X, y)
    for _ in range(10):
        y_proba = net.predict_proba(X)
        print(y_proba)
    

if __name__ == "__main__":
    main()