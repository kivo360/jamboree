from addict import Dict
from sklearn.base import BaseEstimator
from jamboree.middleware.procedures import ModelProcedureAbstract
from sklearn.datasets import make_friedman2
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import DotProduct, WhiteKernel
from loguru import logger




class CremeProcedure(ModelProcedureAbstract):
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



def main():
    import datetime as dt
    from creme import compose
    from creme import datasets
    from creme import feature_extraction
    from creme import linear_model
    from creme import metrics as metricss
    from creme import preprocessing
    from creme import stats
    from creme import stream


    X_y = datasets.Bikes()
    X_y = stream.simulate_qa(X_y, moment='moment', delay=dt.timedelta(minutes=30))

    def add_time_features(x):
        return {
            **x,
            'hour': x['moment'].hour,
            'day': x['moment'].weekday()
        }

    model = add_time_features
    model |= (
        compose.Select('clouds', 'humidity', 'pressure', 'temperature', 'wind') +
        feature_extraction.TargetAgg(by=['station', 'hour'], how=stats.Mean()) +
        feature_extraction.TargetAgg(by='station', how=stats.EWMean())
    )
    model |= preprocessing.StandardScaler()
    model |= linear_model.LinearRegression()

    metric = metricss.MAE()

    questions = {}

    for i, x, y in X_y:
        # Question
        is_question = y is None
        if is_question:
            y_pred = model.predict_one(x)
            questions[i] = y_pred

        # Answer
        else:
            metric.update(y, questions[i])
            model = model.fit_one(x, y)

            if i >= 30000 and i % 30000 == 0:
                print(i, metric)

if __name__ == "__main__":
    main()