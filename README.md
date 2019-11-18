# Jamboree - Fast Event-Driven Library 
An event driven library that is a mix between redis and mongodb. It handles threads in the background and moves relatively fast.


**DO NOT MIND THE POOR DOCS. Everything needs to be worked on later.**


```py
class SampleEnvHandler(DBHandler):
    """Abstract handler that we use to keep track of information.
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.entity = "sample"
        self.required = {
            "episode": str
        }
        self._balance = 0
        self._limit = 100

    @property
    def limit(self):
        return self._limit
    
    @limit.setter
    def limit(self, limit):
        self._limit = limit 

    @property
    def balance(self):
        """ Gets the sum of the last three values at set the value """
        return self._balance

    @property
    def transactions(self)->vaex.dataframe:
        """ Get the last 100 transactions """
        many_records = self.many(self.limit)
        
        if isinstance(many_records, dict):
            frame = pd.DataFrame(many_records)
            transactions_frame = vaex.from_pandas(frame)
            return transactions_frame.sort('timestamp', ascending=False)
        
        if len(many_records) > 0:
            frame = pd.DataFrame(many_records)
            transactions_frame = vaex.from_pandas(frame)
            return transactions_frame.sort('timestamp', ascending=False)
        
        return vaex.from_pandas(pd.DataFrame())

    def save_update_recent(self, data:dict):
        transactions = self.transactions
        count = transactions.count()
        new_value = data['value'] + count
        data['value'] = int(new_value)
        super().save(data)

def flip(n=0.02):
    if n >= random.uniform(0, 1):
        return True
    return False

def main():
    jambo = Jamboree()
    sample_env_handler = SampleEnvHandler()
    sample_env_handler.limit = 250
    sample_env_handler.event = jambo
    sample_env_handler['episode'] = uuid.uuid1().hex
    with timecontext():
        for i in range(1000):
            v1 = randint(0, 12)      
            sample_env_handler.save({"value": v1})
            if flip(0.05):
                sample_env_handler.save_update_recent({"value": v1})
        
        print(sample_env_handler.last())
        print(sample_env_handler.transactions)


if __name__ == "__main__":
    main()
```

## Install
```
pip install jamboree
```
