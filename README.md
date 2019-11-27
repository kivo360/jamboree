# Jamboree: Have a Fast Event-Source System Made with MongoDB, Redis and Love.

**Jamboree is still in Alpha, meaning it should not be used in production systems yet, and it may contain bugs.**

![Logo](docs/jamboree_logo.png)

The goal of jamboree is to have an Event Sourcing Library that stores all prior states of an item located by query key. The purpose of it is to run extremely fast event sourcing for financial transactions from your computer to a large cluster of servers using the exact same code.

Under the hood, the library uses other libraries to like arctic, pebble, redis and mongo. All of these combined help create concurrent transaction.

## Install
```
pip install jamboree
```


## Wait, What is Event Sourcing?

Event Sourcing is a round about way of saying tracking information through their interactions over time more so than exact states. It helps us construct a story of all things that have happened in a system over time. It looks like the image below.

![Event Sourcing](docs/event_sourcing.png)


The ultimate result is that you'd have tracability in your system. This is great when you're trying to see how interactions happen through time.




## How To Read

The Jamboree Library Is Split In Two Parts:

1. Jamboree Event Sourcing
2. Object Handler

The `Jamboree` object is rather simple. It only saves, reads, and deletes records in both `redis` and `mongodb`. Redis to give it fast read times, mongodb as backup to the data. `Handlers` have very explicit storage procedures that interact with the Jamboree object. A good example is the code below. It is from the ![examples/instrument_exchange.py](examples/) directory. The idea is straightforward:

1. We create a Jamboree object. The Jamboree object manages connections to the two databases. 
2. After we create the Handler object, and set the limit (max number of records we want to look at), we start adding records until we stop. At the end, we get the amount of time it took to push the records.
    * Periodically, we do a small calculation to older information prior to adding a record.


If you run the code in instrument exchange, you'll see that 5000 adds to both MongoDB and Redis takes a total of **2.1 seconds** on a single core! For C++ nerds that's nothing, though for the usual developer that's looking to develop an infrastucture, that's fast enough, 2000 adds per second per core, that can also scale horizontally is amazing. They can create server API code around that and create systems that can handle billions of interactions a day. 

```py
jambo = Jamboree()
sample_env_handler = SampleEnvHandler()
sample_env_handler.limit = 250
sample_env_handler.event = jambo
sample_env_handler['episode'] = uuid.uuid1().hex
with timecontext():
    for i in range(10000):
        v1 = randint(0, 12)      
        sample_env_handler.save({"value": v1})
        if flip(0.05):
            sample_env_handler.save_update_recent({"value": v1})
    
    print(sample_env_handler.last())
    print(sample_env_handler.transactions)
```




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

