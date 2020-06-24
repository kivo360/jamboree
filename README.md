# Jamboree: A Fast General Data Engineering Library
 .

![Logo](docs/imgs/jamboree-long-new.png)

**`Jamboree` is extremely early, meaning it should be used with caution. There are plans to improve the system and many components are subject to change. If you look at the improvement plans linked at the bottom you'll be able to see it.**

The goal of `jamboree` was to manage the complexities of data engineering.

## Install

The library requires and `redis` to operate for the time being.

```bash
pip install jamboree
```

## Install Redis

All of the redis installation instructions are [here](https://redis.io/topics/quickstart). Though because the current module setup uses redisearch and will likely use many other modules in the future. Because installing modules is a bit more complex than necessary right now it's best to use docker:

```bash
$ docker run \
    -p 6379:6379 \
    -v /home/{PUTNAMEHERE}/data:/data \
    redislabs/redismod \
    --dir /data
```

**The output should look like the following.**

```bash
1:C 24 Apr 2019 21:46:40.382 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
...
1:M 24 Apr 2019 21:46:40.474 * Module 'ai' loaded from /usr/lib/redis/modules/redisai.so
1:M 24 Apr 2019 21:46:40.474 * <ft> RediSearch version 1.4.7 (Git=)
1:M 24 Apr 2019 21:46:40.474 * <ft> concurrency: ON, gc: ON, prefix min length: 2, prefix max expansions: 200, query timeout (ms): 500, timeout policy: return, cursor read size: 1000, cursor max idle (ms): 300000, max doctable size: 1000000, search pool size: 20, index pool size: 8, 
1:M 24 Apr 2019 21:46:40.475 * <ft> Initialized thread pool!
1:M 24 Apr 2019 21:46:40.475 * Module 'ft' loaded from /usr/lib/redis/modules/redisearch.so
1:M 24 Apr 2019 21:46:40.476 * <graph> Thread pool created, using 8 threads.
1:M 24 Apr 2019 21:46:40.476 * Module 'graph' loaded from /usr/lib/redis/modules/redisgraph.so
loaded default MAX_SAMPLE_PER_CHUNK policy: 360 
1:M 24 Apr 2019 21:46:40.476 * Module 'timeseries' loaded from /usr/lib/redis/modules/redistimeseries.so
1:M 24 Apr 2019 21:46:40.476 # <ReJSON> JSON data type for Redis v1.0.4 [encver 0]
1:M 24 Apr 2019 21:46:40.476 * Module 'ReJSON' loaded from /usr/lib/redis/modules/rejson.so
1:M 24 Apr 2019 21:46:40.476 * Module 'bf' loaded from /usr/lib/redis/modules/rebloom.so
1:M 24 Apr 2019 21:46:40.477 * <rg> RedisGears version 0.2.1, git_sha=fb97ad757eb7238259de47035bdd582735b5c81b
1:M 24 Apr 2019 21:46:40.477 * <rg> PythonHomeDir:/usr/lib/redis/modules/deps/cpython/
1:M 24 Apr 2019 21:46:40.477 * <rg> MaxExecutions:1000
1:M 24 Apr 2019 21:46:40.477 * <rg> RedisAI api loaded successfully.
1:M 24 Apr 2019 21:46:40.477 # <rg> RediSearch api loaded successfully.
1:M 24 Apr 2019 21:46:40.521 * Module 'rg' loaded from /usr/lib/redis/modules/redisgears.so
1:M 24 Apr 2019 21:46:40.521 * Ready to accept connections
```

To run it in the background and let it start when the computer does

```bash
$ docker run \
    -p 6379:6379 -d \
    --restart=always \
    -v /home/{PUTNAMEHERE}/data:/data \
    redislabs/redismod \
    --dir /data
```

## What is Event State Carrying?

State Carrying is a round about way of saying tracking information through their interactions oversp time more so than exact states. It helps us construct a story of all things that have happened in a system over time. It looks like the image below.

![Event Sourcing](docs/imgs/event-sourcing_long.png)

State carrying is dragging the current state along over time.

The ultimate result is that you'd have tracability in your system. This is great when you're trying to see how interactions happen through time.

## How The Library Works

The Jamboree Library Is Split In Two Parts:

1. Jamboree Event Sourcing
2. Object Handler

The `Jamboree` object is rather simple. It only saves, reads, and deletes records in both `redis` and `mongodb`. Redis to give it fast read times, mongodb as backup to the data. `Handlers` have very explicit storage procedures that interact with the Jamboree object. A good example is the code below.

The idea is straightforward:

1. We create a `Jamboree` object. The Jamboree object manages connections to databases at a high speed and low latency.
2. After we create the Handler object, and set the limit (max number of records we want to look at), we start adding records until we stop. At the end, we get the amount of time it took to push the records.
    * Periodically, we do a small calculation to older information prior to adding a record.

## Creating a Handler

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

if __name__ == "__main__":
    main()
```


## Timing The Handler

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
```

## Improvement Plans

Jamboree currently has a list of improvements that

https://trello.com/b/9vwpc5C6