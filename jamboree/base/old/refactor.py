import base64
import random
from abc import ABC
from copy import copy
from multiprocessing import cpu_count
from typing import List

import maya
import orjson
import ujson
from loguru import logger
from pebble.pool import ThreadPool
from redis import Redis

from funtime import Store
from jamboree.storage.databases import (MongoDatabaseConnection,
                                        RedisDatabaseConnection)


class EventProcessor(ABC):
    def save(self, query: dict, data: dict, abs_rel="absolute"):
        raise NotImplementedError


    def save_many(self, query: dict, data: List[dict], abs_rel="absolute"):
        raise NotImplementedError    


    def get_latest(self, query, abs_rel="absolute") -> dict:
        raise NotImplementedError


    def get_latest_many(self, query, abs_rel="absolute", limit=1000):
        raise NotImplementedError


    def get_between(self, query:dict, min_epoch:float, max_epoch:float, abs_rel:str="absolute") -> list:
        raise NotImplementedError


    def get_latest_by(self, query:dict, max_epoch, abs_rel="absolute", limit:int=10) -> dict:
        raise NotImplementedError


    def count(self, query: dict) -> int:
        raise NotImplementedError


    def remove_first(self, query: dict):
        raise NotImplementedError

    
    def pop_multiple(self, query: dict, limit: int):
        raise NotImplementedError


    def _bulk_save(self, query: dict, data: list):
        raise NotImplementedError


    def single_get(self, query:dict):
        raise NotImplementedError


    def single_set(self, query:dict, data:dict):
        raise NotImplementedError


    def single_delete(self, query:dict):
        raise NotImplementedError


class Jamboree(EventProcessor):
    """Adds and retrieves events at extremely fast speeds. Use to handle portfolio and trade information quickly."""

    def __init__(self, mongodb_host="localhost", redis_host="localhost", redis_port=6379):
        self.redis = Redis(redis_host, port=redis_port)
        self.store = Store(mongodb_host).create_lib('events').get_store()['events']
        self.pool = ThreadPool(max_workers=cpu_count() * 4)
        self.mongo_conn = MongoDatabaseConnection()
        self.redis_conn = RedisDatabaseConnection()
        self.mongo_conn.connection = self.store
        self.redis_conn.connection = self.redis
        # self.redis_conn.pool = self.pool
        # self.mongo_conn.pool = self.pool

    def _validate_query(self, query: dict):
        """ Validates a query. Must have `type` and a second identifier at least"""
        if 'type' not in query:
            return False
        if not isinstance(query['type'], str):
            return False
        if len(query) < 2:
            return False
        return True

    def _generate_hash(self, query: dict):
        _hash = ujson.dumps(query, sort_keys=True)
        _hash = base64.b64encode(str.encode(_hash))
        _hash = _hash.decode('utf-8')
        return _hash

    def _check_redis_for_prior(self, _hash: str) -> bool:
        """ Checks to see if any """
        prior_length = self.redis.llen(_hash)
        if prior_length == 0:
            return False
        return True

    def _update_dict(self, query: dict, data: dict):
        query = copy(query)
        timestamp = maya.now()._epoch
        query['timestamp'] = timestamp
        data.update(query)
        return data

    def _update_dict_no_timestamp(self, query: dict, data: dict):
        query = copy(query)
        data = copy(data)
        data.update(query)
        data.pop("timestamp", None)
        return data

    def _omit_timestamp(self, data: dict):
        """ Removes timestamp if it exists. Use it to create a copied version of a dictionary to be saved in the duplicate list """
        _data = copy(data)
        _data.pop("timestamp", None)
        return _data

    def back_to_dict(self, list_of_serialized: list):
        deserialized = []
        if len(list_of_serialized) == 1:
            return orjson.loads(list_of_serialized[0])

        for i in list_of_serialized:
            deserialized.append(orjson.loads(i))
        return deserialized

    def _save(self, query: dict, data: dict):
        """
            Given a type (data entity), data and a epoch for time (utc time only), save the data in both redis and mongo. 
            Does it in a background process. Use with add event.
            We save the information both in mongodb and redis. We assume there's many of each collection. We find a specific collection using the query.
        """
        self.redis_conn.save(query, data)
        self.pool.schedule(self.mongo_conn.save, args=(query, data))

    """
        RESET FUNCTIONS
    """

    def _reset_count(self, query: dict):
        """ Reset the count for the current mongodb query. We do this by adding records in mongo back into redis. """
        all_elements = self.mongo_conn.query_all(query)
        self.pool.schedule(self.redis_conn.reset, args=(query, all_elements))

    def reset(self, query: dict):
        """ Resets all of the variables """
        self.pool.schedule(self._reset_count, args=(query))

    """
        DELETES FUNCTIONS
    """

    def _remove(self, query: dict, details: dict):
        """ Use to both remove items from redis and mongo. Add it when you need it."""

        """ 
            Removes the given query information from the database. 
            It's a heavy computation on redis, as it'll require searching an entire list.
            
        """
        self.pool.schedule(self.mongo_conn.delete_all, args=(query, details))
        self.redis_conn.delete(query, details)
        self.pool.schedule(self.redis_conn.delete_all, args=(query))

    def _remove_first_redis(self, _hash, query: dict):
        # rlock = f"{_hash}:lock"
        # with self.redis.lock(rlock):
        #     push_key = f"{_hash}:list"
        #     self.redis.rpop(push_key)
        pass

    def remove_first(self, query: dict):
        pass
        # _hash = self._generate_hash(query)
        # count = self._get_count(_hash, query)

        # if count == 0:
        #     return

        # self._remove_first_redis(_hash, query)

    """ 
        SAVE FUNCTIONS
    """

    def save(self, query: dict, data: dict):
        self._save(query, data)

    def save_many(self, query: dict, data: List[dict]):
        if self._validate_query(query) == False:
            # Log a warning here instead
            return

        if len(data) == 0:
            return

        for item in data:
            self._save(query, item)

    def bulk_upsert_redis(self, query, data):
        logger.info("Default retcon redis")
        self.pool.schedule(self.redis_conn.update_many, args=(query, data))

    def _bulk_save(self, query, data: list):
        """ Bulk adds a list to redis."""

        self.redis_conn.save_many(query, data)
        self.pool.schedule(self.mongo_conn.save_many, args=(query, data))

    def _save_redis(self, _hash: str, data: dict):

        serialized = orjson.dumps(data)
        rlock = f"{_hash}:lock"
        with self.redis.lock(rlock):
            push_key = f"{_hash}:list"
            self.redis.rpush(push_key, serialized)

    def _bulk_save_redis(self, _hash: str, data: list):
        serialized_list = [orjson.dumps(x) for x in data]

        rlock = f"{_hash}:lock"
        with self.redis.lock(rlock):
            push_key = f"{_hash}:list"
            self.redis.rpush(push_key, *serialized_list)

    def _pop_redis_multiple(self, _hash, limit: int):
        rlock = f"{_hash}:lock"
        with self.redis.lock(rlock):
            with self.redis.pipeline() as pipe:
                latest_items = []
                try:
                    push_key = f"{_hash}:list"
                    pipe.watch(push_key)
                    latest_items = pipe.lrange(push_key, -limit, -1)
                    pipe.ltrim(push_key, 0, -limit)
                    pipe.execute()

                except Exception as e:
                    pass
                finally:
                    pipe.reset()
                if len(latest_items) > 0:
                    return self.back_to_dict(latest_items)
                return latest_items

    def pop_multiple(self, query, limit: int = 1):
        """ Get multiple items """
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        if count == 0:
            return []
        return self._pop_redis_multiple(_hash, limit)

    """
        Public Query Functions
    """

    def query_direct(self, query):
        """ Queries from mongodb directly. Used to search extremely large queries. """
        latest_items = list(self.store.query_latest(query))
        return latest_items

    def query_direct_latest(self, query):
        """ Queries from mongodb directly. Used to search extremely large queries. """
        latest_items = list(self.store.query_latest(query))
        if len(latest_items) > 0:
            return latest_items[0]
        return {}

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

    def get_latest_many(self, query: dict, limit=1000):

        if self._validate_query(query) == False: return []
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        if count == 0: return []

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

    """
        SEARCH ONE FUNCTIONS
    """

    def _search_one(self, item: dict, query: dict):
        all_bools = []
        for q in query:
            if q in item:
                if query[q] == query[q]:
                    all_bools.append(True)
                else:
                    all_bools.append(False)
            else:
                all_bools.append(False)
        return any(all_bools)

    def _get_count(self, _hash: str, query: dict):
        # Checks to see if a count already exist in redis, if not, check for a count in mongo.
        _count_hash = f"{_hash}:list"
        count = self.redis.llen(_count_hash)
        if count is not None:
            return count

        records = list(self.store.query(query))
        record_len = len(records)
        return record_len

    def count(self, query):
        """ """
        if self._validate_query(query) == False: return []
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        return count
