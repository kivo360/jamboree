from copy import copy
import orjson
import ujson
from typing import List
from redis import Redis
from funtime import Store
from pebble.pool import ThreadPool
import base64
from multiprocessing import cpu_count
from jamboree.storage.databases import MongoDatabaseConnection, ZRedisDatabaseConnection
from jamboree.utils.helper import Helpers
from jamboree.base.processors.abstracts import EventProcessor
from jamboree.base.processors.abstracts import LegacyProcessor




class Jamboree(EventProcessor):
    """Adds and retrieves events at extremely fast speeds. Use to handle portfolio and trade information quickly."""

    def __init__(self, mongodb_host="localhost", redis_host="localhost", redis_port=6379):
        self.redis = Redis(redis_host, port=redis_port)
        self.store = Store(mongodb_host).create_lib('events').get_store()['events']
        self.pool = ThreadPool(max_workers=cpu_count() * 6)
        self.mongo_conn = MongoDatabaseConnection()
        self.redis_conn = ZRedisDatabaseConnection()
        self.mongo_conn.connection = self.store
        self.redis_conn.connection = self.redis
        self.dominant_database = ""
        self.helpers = Helpers()
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

    def _save(self, query: dict, data: dict, abs_rel="absolute"):
        """
            # TODO: Readd mongo
            Given a type (data entity), data and a epoch for time (utc time only), save the data in both redis and mongo. 
            Does it in a background process. Use with add event.
            We save the information both in mongodb and redis. We assume there's many of each collection. We find a specific collection using the query.
        """
        self.redis_conn.save(query, data)
        # self.pool.schedule(self.mongo_conn.save, args=(query, data))

    """
        RESET FUNCTIONS
    """

    def _reset_count(self, query: dict):
        """ Reset the count for the current mongodb query. We do this by adding records in mongo back into redis. """
        # all_elements = self.mongo_conn.query_all(query)
        # self.pool.schedule(self.redis_conn.reset, args=(query, all_elements))

    def reset(self, query: dict):
        """ Resets all of the variables """
        # self.pool.schedule(self._reset_count, args=(query))

    """
        DELETES FUNCTIONS
    """

    def _remove(self, query: dict, details: dict):
        """ Use to both remove items from redis and mongo. Add it when you need it."""

        """ 
            Removes the given query information from the database. 
            It's a heavy computation on redis, as it'll require searching an entire list.
            
        """
        # self.pool.schedule(self.mongo_conn.delete_all, args=(query, details))
        self.redis_conn.delete(query, details)
        self.pool.schedule(self.redis_conn.delete_all, args=(query))

    def _remove_first_redis(self, _hash, query: dict):
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

    def save(self, query: dict, data: dict, abs_rel="absolute"):
        self._save(query, data)

    def save_many(self, query: dict, data: List[dict]):
        if self._validate_query(query) == False:
            # Log a warning here instead
            return

        if len(data) == 0: return
        data_list = [self.helpers.update_dict(query, item) for item in data]
        self._bulk_save(query, data_list)


    def _bulk_save(self, query, data: list):
        """ Bulk adds a list to redis."""
        events = self.helpers.convert_to_storable_relative(data)
        self.redis_conn.save_many(query, events)
        # self.pool.schedule(self.mongo_conn.save_many, args=(query, data))


    def _pop_redis_multiple(self, _hash, limit: int):
        return []

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
        return self.mongo_conn.query_latest(query)

    def get_latest(self, query, abs_rel="absolute"):
        """ Gets the latest query"""
        # Add a conditional time lock
        _hash = self._generate_hash(query)
        count, database = self._get_count(_hash, query)
        if count > 0:
            return self.redis_conn.query_latest(query, abs_rel)
        return {}
        # # Mongo, slowdown
        # return self.mongo_conn.query_latest(query)

    def get_latest_many(self, query: dict, limit=1000, abs_rel="absolute"):

        if self._validate_query(query) == False: return []
        _hash = self._generate_hash(query)
        count = self._get_count(_hash, query)
        if count == 0: return []



        latest_redis_items = self.redis_conn.query_latest_many(query, abs_rel=abs_rel, limit=limit)
        return latest_redis_items


    """ New Queries """


    def get_between(self, query:dict, min_epoch:float, max_epoch:float, abs_rel:str="absolute"):
        items = self.redis_conn.query_between(query, min_epoch, max_epoch, abs_rel)
        return items
    

    def get_latest_by(self, query:dict, max_epoch, abs_rel="absolute", limit:int=10):
        item = self.redis_conn.query_latest_by_time(query, max_epoch, abs_rel)
        return item

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
        count = self.redis_conn.count(_hash)
        if count is not None:
            return count, "redis"

        records = list(self.store.query(query))
        record_len = len(records)
        return record_len, "mongo"

    def count(self, query):
        """ """
        if self._validate_query(query) == False: return []
        _hash = self._generate_hash(query)
        count, database = self._get_count(_hash, query)
        self.dominant_database = database
        return count

    def single_get(self, query:dict):
        if self._validate_query(query) == False: return {}
        item = self.redis_conn.get(query)
        return item 

    def single_set(self, query:dict, data:dict):
        if self._validate_query(query) == False: return
        self.redis_conn.add(query, data)

    def single_delete(self, query:dict):
        if self._validate_query(query) == False: return
        self.redis_conn.kill(query)