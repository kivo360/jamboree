# Procedures

Procedures are abstracts that we use to call things in consistent ways. The core example is a model procedure. Multiple different models are called in multiple different ways yet they can look extremely consistent on the surface. For example, calling a `fit` and `partial_fit` can look the same for all major machine learning libraries. 

Let's compare using `sklearn` and `creme-ml` for a basic fit example.


```py
class SKLearnProcedure(object):
    def __init__(self, *args, **kwargs):
        pass
    
    def partial_fit(self, data):
        pass

    def fit(self, data):
        pass



class CremeProcedure(object):
    def __init__(self, *args, **kwargs):
        pass
    
    def partial_fit(self, data):
        # All steps goes here
        pass

    def fit(self, data):
        # All steps goes here
        pass
```


We can call the exact same calls using the exact same data and get exactly what we need.


```py
data = pd.DataFrame()

creme_model = CremeProcedure()
sklearn_model = SKlearnProcedure()


sklearn_model.fit(data)
creme_model.fit(data)
```

Each one of these models will have procedures to handle what's inputted into them.