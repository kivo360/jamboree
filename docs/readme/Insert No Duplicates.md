# Inserting Data Without Duplicates

Here we test inserting data without duplicates. Afterwards we'll test for missing data inside of the databases.


```python
from jamboree import Jamboree
import pandas as pd
import datetime
import pandas_datareader.data as web
from pandas import Series, DataFrame
```


```python
from maya import MayaDT
import maya
import copy
```


```python
import random
import orjson
```


```python
from typing import List, Dict, Any
```


```python
jam_session = Jamboree()
```

    Unable to create library with name: events



    ---------------------------------------------------------------------------

    LibraryNotFoundException                  Traceback (most recent call last)

    <ipython-input-7-00a3bcc8c141> in <module>
    ----> 1 jam_session = Jamboree()
    

    ~/PycharmProjects/jamboree/jamboree/base/main.py in __init__(self, mongodb_host, redis_host, redis_port)
         55     def __init__(self, mongodb_host="localhost", redis_host="localhost", redis_port=6379):
         56         self.redis = Redis(redis_host, port=redis_port)
    ---> 57         self.store = Store(mongodb_host).create_lib('events').get_store()['events']
         58         self.pool = ThreadPool(max_workers=cpu_count() * 4)
         59 


    ~/.local/lib/python3.6/site-packages/arctic/arctic.py in __getitem__(self, key)
        373     def __getitem__(self, key):
        374         if isinstance(key, string_types):
    --> 375             return self.get_library(key)
        376         else:
        377             raise ArcticException("Unrecognised library specification - use [libraryName]")


    ~/.local/lib/python3.6/site-packages/arctic/arctic.py in get_library(self, library)
        358         if error:
        359             raise LibraryNotFoundException("Library %s was not correctly initialized in %s.\nReason: %r)" %
    --> 360                                            (library, self, error))
        361         elif not lib_type:
        362             raise LibraryNotFoundException("Library %s was not correctly initialized in %s." %


    LibraryNotFoundException: Library events was not correctly initialized in <Arctic at 0x7efbaa926128, connected to MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True, maxpoolsize=4, sockettimeoutms=600000, connecttimeoutms=2000, serverselectiontimeoutms=30000)>.
    Reason: ServerSelectionTimeoutError('localhost:27017: [Errno 111] Connection refused',))



```python
start = datetime.datetime(1986, 3, 14)
end = datetime.datetime(2020, 1, 6)
```


```python
apple_df = web.DataReader("AAPL", 'yahoo', start, end)
msft_df = web.DataReader("MSFT", 'yahoo', start, end)
```


```python
apple_df
```


```python
def get_year_month_day(time:MayaDT):
    print(f"{time.day}-{time.month}-{time.year}")
```


```python
def get_time_dt(df):
    indexes = df.index
    indexes = [maya.MayaDT.from_datetime(index.to_pydatetime()) for index in indexes]
    return indexes
```


```python
def df_records(df):
    return df.to_dict("records")
```


```python
def standardize_record(record):
    closing_record = {}
    if "Close" in record:
        closing_record['close'] = record["Close"]
    if "Open" in record:
        closing_record['open'] = record["Open"]
    if "Low" in record:
        closing_record['low'] = record["Low"]
    if "High" in record:
        closing_record['high'] = record["High"]
    if "Volume" in record:
        closing_record['volume'] = record["Volume"]
    
    return closing_record
```


```python
def standardize_outputs(records:List[Dict[str, Any]]):
    if len(records) == 0:
        return []
    _records = [standardize_record(rec) for rec in records]
    return _records
```


```python
def add_time(records, times):
    if len(records) == 0 or (len(records) != len(times)):
        return []
    
    _records = []
    for index, rec in enumerate(records):
        rec['time'] = times[index]._epoch
        _records.append(rec)
    return _records
```


```python
def teardown(df):
    """Breaks the dataframe into a bunch of dictionaries"""
    indexes = get_time_dt(df)
    records = df_records(df)
    standardized = standardize_outputs(records)
#     print(standardized)
    with_time = add_time(standardized, indexes)
    return with_time
```


```python
dt_time = teardown(apple_df)
```


```python
def flip(n=0.05):
    if random.uniform(0, 1) < n:
        return True
    return False
```


```python
def create_duplicates(frame_dict_list:List[Dict]):
    if len(frame_dict_list) == 0:
        return []
    
    final_list = []
    for item in frame_dict_list:
        final_list.append(item)
        if flip(0.1):
            final_list.append(item)
    return final_list
```


```python

```


```python
# len(dups)
```


```python
last_200 = dt_time[-200:]
last_300 = dt_time[-300:]
last_200_dups = create_duplicates(last_200)
last_300_dups = create_duplicates(last_300)
```


```python
upsert_data_one = jam_session.bulk_upsert_redis({"type": "sample_save", "asset": "AAPL", "label": "duplication"}, last_200)
upsert_data_two = jam_session.bulk_upsert_redis({"type": "sample_save", "asset": "AAPL", "label": "duplication"}, last_300)
upsert_data_one_dups = jam_session.bulk_upsert_redis({"type": "sample_save", "asset": "AAPL", "label": "duplication"}, last_200_dups)
upsert_data_two_dups = jam_session.bulk_upsert_redis({"type": "sample_save", "asset": "AAPL", "label": "duplication"}, last_300_dups)
```


```python
main_hash = upsert_data_one.get("hash")
```


```python
up1 = upsert_data_one.get('updated', [])
up2 = upsert_data_two.get('updated', [])
up3 = upsert_data_one_dups.get('updated', [])
up4 = upsert_data_two_dups.get('updated', [])
```


```python
cr1 = [orjson.dumps(x) for x in up1]
cr2 = [orjson.dumps(x) for x in up2]
cr3 = [orjson.dumps(x) for x in up3]
cr4 = [orjson.dumps(x) for x in up4]
```


```python
set1 = set(cr1)
set2 = set(cr2)
set3 = set(cr3)
set4 = set(cr4)
```


```python
print(len(set1))
print(len(set2))
print(len(set3))
print(len(set4))
```


```python
jam_session.redis.sadd(set_key, *set(cr3))
```


```python

```


```python
def deserialize_list(serialized_list:list):
    if len(serialized_list) == 0:
        return []
    
    return [orjson.loads(x) for x in serialized_list]
```


```python
def add_timestamp(item):
    item['timestamp'] = maya.now()._epoch
    return item
```


```python
def get_addable_items(set_key, added_set):
    existing = set(jam_session.redis.smembers(set_key))
    addable_items = set(set2 - existing)
    if len(addable_items) == 0:
        return []
    listified = list(addable_items)
    deku = deserialize_list(listified)
    timestamped = [add_timestamp(x) for x in deku]
    return timestamped
```


```python
# updated_set = set(serialized_updated)
```


```python
get_addable_items(set_key, set2)
```


```python
# jam_session.redis.smembers(set_key, 0, -1)
```


```python
set(retrieved - updated_set)
```


```python
len(retrieved)
```


```python
len(updated_set)
```


```python

```
