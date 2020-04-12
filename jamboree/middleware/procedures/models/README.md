# Machine Learning Interaction Procedures


Forces all ML libraries to be accessed using the exact same sklearn like API.


* `predict(X, **params)`
* `pred_proba(X, y, **params)`
* `fit(X, y, **params)`
* `partial_fit(X, y, **params)`
* `adjust(X, y, **params)`
* `.metrics` - Gets the metrics of the model. `X` and `y` are supposed to be taken from the adjust column.
* `get_params()`
* `set_params(**params)` 
* `extract()` - Gets the full model in a storable format

Will need to run through some walk-forward testing examples after the backtest is done.