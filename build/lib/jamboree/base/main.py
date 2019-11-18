from abc import ABC
import maya
import orjson
import ujson
from typing import List, Dict
from redis import Redis
from funtime import Store
from pebble import concurrent
import base64

class EventProcessor(ABC):
    def save(self, query:dict, data:dict):
        raise NotImplementedError

    def get_latest(self, query):
        raise NotImplementedError
    
    def get_latest_many(self, query, limit=1000):
        raise NotImplementedError
    
    def save_many(self, query:dict, data:List[dict]):
        raise NotImplementedError

class Jamboree(EventProcessor):
    """Adds and retrieves events at extremely fast speeds. Use to handle portfolio and trade information quickly."""
    def __init__(self, mongodb_host="localhost", redis_host="localhost", redis_port=6379):
        self.redis = Redis(redis_host, port=redis_port)
        self.store = Store(mongodb_host).create_lib('events').get_store()['events']
    
    # def _redis_lock(self, _hash:str):
    #     rlock = f"{_hash}:lock"
    #     return self.redis.Lock(self.redis, rlock)

    
    def _validate_query(self, query:dict):
        """ Validates a query. Must have `type` and a second identifier at least"""
        if 'type' not in query:
            return False
        if not isinstance(query['type'], str):
            return False
        if len(query) < 2:
            return False
        return True
    
    def _generate_hash(self, query:dict):
        _hash = ujson.dumps(query, sort_keys=True)
        _hash = base64.b64encode(str.encode(_hash))
        _hash = _hash.decode('utf-8')
        # print(_hash)
        return _hash

    def _check_redis_for_prior(self, _hash:str) -> bool:
        """ Checks to see if any """
        prior_length = self.redis.llen(_hash)
        if prior_length == 0:
            return False
        return True


    def _save_redis(self, _hash:str, data:dict):
        serialized = orjson.dumps(data)
        rlock = f"{_hash}:lock"
        with self.redis.lock(rlock):
            push_key = f"{_hash}:list"
            self.redis.rpush(push_key, serialized)

    @concurrent.thread
    def _save_mongo(self, data):
        self.store.store(data)


    def back_to_dict(self, list_of_serialized:list):
        deserialized = []
        if len(list_of_serialized) == 1:
            return orjson.loads(list_of_serialized[0])
        
        for i in list_of_serialized:
            
            deserialized.append(orjson.loads(i))
        return deserialized

    
    def _get_count(self, _hash:str, query:dict):
        # Checks to see if a count already exist in redis, if not, check for a count in mongo.
        _count_hash = f"{_hash}:list"
        count = self.redis.llen(_count_hash)
        if count is not None:
            return count
        # Warning slow!!!
        records = list(self.store.query(query))
        record_len = len(records)
        return record_len


    def _update_count(self, _hash:str, count:int):
        _count_hash = f"{_hash}:count"        
        self.redis.set(_count_hash, count)


    @concurrent.thread
    def _reset_count(self, query:dict):
        """ Reset the count for the current mongodb query"""
        _hash = self._generate_hash(query)
        phindex = self.redis.incr("placeholder:index")
        delindex = self.redis.incr("deletion:index")
        _hash_key = f"{_hash}:list" 
        _hash_placeholder = f"{_hash}:{phindex}"
        _hash_del = f"{_hash}:{delindex}"


        # self.redis.rename(_hash_key, _hash_rename)
        mongo_data = list(self.store.query(query))
        
        # placeholder key
        for md in mongo_data:
            self.redis.rpush(_hash_placeholder, orjson.dumps(md))


        self.redis.rename(_hash_key, _hash_del)
        self.redis.rename(_hash_placeholder, _hash_key)
        while self.redis.llen(_hash_del) > 0:
            self.redis.ltrim(_hash_del, 0, -99)

    def _save(self, query:dict, data:dict):
        """
            Given a type (data entity), data and a epoch for time (utc time only), save the data in both redis and mongo. 
            Does it in a background process. Use with add event.
            We save the information both in mongodb and redis. We assume there's many of each collection. We find a specific collection using the query.
        """
        if self._validate_query(query) == False:
            # Log a warning here instead
            return
        timestamp=maya.now()._epoch
        _hash = self._generate_hash(query)
        # Now time to update the system
        query.update(data)
        query['timestamp'] = timestamp
        
        self._save_redis(_hash, query)
        self._save_mongo(query)

    def reset(self, query:dict):
        """ Resets all of the variables """
        if self._validate_query(query) == False:
            # Log a warning here instead
            return
        self._reset_count(query)



    def save(self, query:dict, data:dict):
        # print("Query")
        self._save(query, data)
    
    
    def save_many(self, query:dict, data:List[dict]):
        if self._validate_query(query) == False:
            # Log a warning here instead
            return

        if len(data) == 0:
            return
        
        for item in data:
            self._save(query, item)


    def get_latest(self, query):
        """ Gets the latest query"""
        # Add a conditional time lock
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        if count > 0:
            return self.back_to_dict(self.redis.lrange(f"{_hash}:list", -1, -1))
        # Mongo, slowdown
        latest_items = list(self.store.query_latest(query))
        if len(latest_items) > 0:
            return latest_items[0]
        return {}
    

    def get_latest_many(self, query:dict, limit=1000):

        if self._validate_query(query) == False:
            # Log a warning here instead
            # print("False query")
            return []
        
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        if count == 0:
            return []
        
        latest_redis_items = self.back_to_dict(self.redis.lrange(f"{_hash}:list", -limit, -1))
        # TODO: Get mongo tasks here
        # How will this work now?
        rlen = len(latest_redis_items)
        if rlen == 0:
            query["limit"] = limit
            latest_items = list(self.store.query_latest(query))
            self.reset(query)
            return latest_items


        return latest_redis_items
        
        