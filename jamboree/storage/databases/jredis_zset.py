import maya
import orjson
from copy import copy

from redis.client import Pipeline, Redis
from typing import Dict, List, Optional, Any, AnyStr, Union
from pprint import pprint
from jamboree.storage.databases import DatabaseConnection

# from redis.exceptions import WatchError
from jamboree.utils.context import watch_loop, watch_loop_callback

# NOTE: Can add pipelining to redis storage to make fewer calls.
from loguru import logger
import redis

# def default()


class RedisDatabaseZSetsConnection(DatabaseConnection):
    """ 
        Redis Connection Notes
        ---
        We're going to end up using zadds and zscores. 
        The indexable score makes it a good target for time series. 
        The fact that it's a set makes it so there can be no duplicates added.

        In essense. Perfect for time series. 
    """

    def __init__(self) -> None:
        super().__init__()

    """
        # Individual Access Methods
        ---
        Use commands here to have single variable in place instead of creating an event log. 
        Very useful for stateful applications such as timeindexing logic.

        ## Core Functions:

        * kill - delete a given hash/key
        * add  - set a given hash/key to a variable. Usually that should be a dictionary representing a complex value.
        * get  - get a variable by hash/key
    """

    def _kill(self, _hash: str):
        """ Deletes a key within a lock"""
        rlock = f"{_hash}:lock"
        sub_key = f"{_hash}:single"
        with self.connection.pipeline() as pipe:
            with pipe.lock(rlock):
                pipe.delete(sub_key)
            pipe.execute()

    def _add(self, _hash: str, data: dict, is_serialized=True):
        """Set a single variable to the redis database using the redis set command. 

        Arguments:
            _hash {str} -- The hash identifier
            data {dict} -- The data we're going to add.

        Keyword Arguments:
            is_serialized {bool} -- Determine if we're to serialize the underlying variable. (default: {False})
        """
        rlock = f"{_hash}:lock"
        sub_key = f"{_hash}:single"
        serialized = None
        if is_serialized:
            # Don't serialize the json
            serialized = orjson.dumps(data, option=orjson.OPT_SERIALIZE_NUMPY)
        else:
            serialized = data
        with self.connection.pipeline() as pipe:
            with pipe.lock(rlock):
                pipe.set(sub_key, serialized)
            pipe.execute()

    def _get(self, _hash: str):
        sub_key = f"{_hash}:single"
        value = None
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(sub_key)
                    value = pipe.get(sub_key)
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

        return value

    def get(self, query: dict, is_serialized=True):
        if not self.helpers.validate_query(query):
            return {}

        _hash = self.helpers.generate_hash(query)
        value = self._get(_hash)
        if value is not None:
            if is_serialized:
                return orjson.loads(value)
            return value
        return {}

    def add(self, query: dict, data: dict, is_serialized=True):
        """ Sets a single value. It's a wrapper around"""
        if not self.helpers.validate_query(query):
            if is_serialized:
                if len(data) == 0:
                    return

        _hash = self.helpers.generate_hash(query)
        self._add(_hash, data, is_serialized=is_serialized)

    def kill(self, query: dict):
        """ Deletes a single variable. Bypasses the stack"""
        if not self.helpers.validate_query(query):
            return

        _hash = self.helpers.generate_hash(query)
        self._kill(_hash)

    """ 
        # Save Commands
        ---
        * `save` - ...
        * `save_many` - ...
    """

    @logger.catch
    def _save(self, _hash: str, data: dict, timing: dict):
        """ Appends an event to the stack. """
        serialized = orjson.dumps(data, option=orjson.OPT_SERIALIZE_NUMPY)
        rlock = f"{_hash}:lock"
        relative_time_key = f"{_hash}:rlist"
        absolute_time_key = f"{_hash}:alist"
        with self.connection.pipeline() as pipe:
            with pipe.lock(rlock):
                relative_data = {serialized: timing["time"]}
                absolute_data = {serialized: timing["timestamp"]}
                pipe.zadd(relative_time_key, relative_data)
                pipe.zadd(absolute_time_key, absolute_data)
            pipe.execute()

    def save(self, query: dict, data: dict, _time=None, _timestamp=None):
        """ Save a single record. """
        if not self.helpers.validate_query(query):
            return
        _hash = self.helpers.generate_hash(query)
        query.update(data)
        data, timing = self.helpers.separate_time_data(query, _time, _timestamp)
        # print(timing)
        self._save(_hash, data, timing)

    def _save_many(self, _hash: str, relative_data: Dict[str, float] = {}):
        # serialized_list = [orjson.dumps(x) for x in data]

        rlock = f"{_hash}:lock"
        relative_time_key = f"{_hash}:rlist"
        absolute_time_key = f"{_hash}:alist"
        absolute_data = self.helpers.get_current_abs_time(relative_data)
        with self.connection.pipeline() as pipe:
            with pipe.lock(rlock):
                pipe.zadd(relative_time_key, relative_data)
                pipe.zadd(absolute_time_key, absolute_data)
            pipe.execute()

    def save_many(self, query, data: Dict[str, float] = {}, abs_rel="absolute"):
        """ 
            # Save Many

            Save multiple records at once. We assume the data was already processed. 
        """
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        _hash = self.helpers.generate_hash(query)
        self._save_many(_hash, data)

    """ 
        # Delete Commands
        ---
        * `delete` - Get all of the records related to a given key.
        * `delete_latest` - Get the `n` latest records according to our query parameters.
        * `delete_between` - Query between two epoch times.
        * `delete_before` - Get everything before an epoch time.
        * `delete_after` - Get everything after epoch time.

    """

    def delete_first(self, query, details):
        pass

    def _delete(self, _hash: str, details: dict):
        rlock = f"{_hash}:lock"
        relative_time_key = f"{_hash}:rlist"
        absolute_time_key = f"{_hash}:alist"
        deletion_key = orjson.dumps(details, option=orjson.OPT_SERIALIZE_NUMPY)
        with self.connection.lock(rlock):
            self.connection.zrem(relative_time_key, deletion_key)
            self.connection.zrem(absolute_time_key, deletion_key)

    def delete(self, query: dict, details: dict):
        if not self.helpers.validate_query(query):
            return
        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0:
            return
        self._delete(_hash, details)

    def _delete_many(self, _hash: str, data: List[Dict]):
        pass

    def delete_many(self, query, data: List[Dict]):
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        updated_list = [self.connection.update_dict(query, x) for x in data]
        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0:
            return
        self._delete_many(_hash, updated_list)

    def _delete_all(self, _hash: str):
        rlock = f"{_hash}:lock"
        relative_time_key = f"{_hash}:rlist"
        absolute_time_key = f"{_hash}:alist"

        with self.connection.lock(rlock):
            keys = self.connection.zrange(relative_time_key, 0, -1, withscores=True)

            values = [key[0] for key in keys]
            if len(values) == 0:
                return

            self.connection.zrem(relative_time_key, *values)
            self.connection.zrem(absolute_time_key, *values)

    def delete_all(self, query: dict):
        if not self.helpers.validate_query(query):
            return

        _hash = self.helpers.generate_hash(query)
        self._delete_all(_hash)

    """
        Query commands
        ---
        All of the query information we need to operate on.


        The query functions we work on:

        1. `query` - Get all of the records related to a given key.
        2. `query_latest` - Get the `n` latest records according to our query parameters.
        3. `query_between` - Query between two epoch times.
        4. `query_before` - Get everything before an epoch time.
        5. `query_after` - Get everything after epoch time.    
    """

    def query_all(self, query: dict):
        """ Same as query_all """
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        relative_time_key = f"{_hash}:rlist"
        absolute_time_key = f"{_hash}:alist"
        with self.connection.pipeline() as pipe:
            akeys = None
            rkeys = None
            while True:
                try:
                    pipe.watch(relative_time_key)
                    pipe.watch(absolute_time_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return []

                    rkeys = pipe.zrange(relative_time_key, 0, -1, withscores=True)
                    akeys = pipe.zrange(absolute_time_key, 0, -1, withscores=True)
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        combined = self.helpers.combine_results(akeys, rkeys)
        return combined

    def query_latest(self, _query, abs_rel="absolute", limit: int = 10):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return {}
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)

        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return {}

                    keys = pipe.zrange(_current_key, -1, -1, withscores=True)
                    if len(keys) == 0:
                        return {}
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        combined = self.helpers.combined_abs_rel(keys, abs_rel=abs_rel)
        return combined[-1]

    def query_latest_many(self, _query, abs_rel="absolute", limit: int = 10):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return {}
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return {}

                    keys = pipe.zrange(_current_key, -limit, -1, withscores=True)
                    if len(keys) == 0:
                        return {}
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

        combined = self.helpers.combined_abs_rel(keys, abs_rel=abs_rel)
        return combined

    def query_between(
        self,
        _query: dict,
        min_epoch: float,
        max_epoch: float,
        abs_rel: str = "absolute",
    ):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return []
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)

        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return []

                    keys = pipe.zrangebyscore(
                        _current_key, min=min_epoch, max=max_epoch, withscores=True
                    )
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        combined = self.helpers.combined_abs_rel(keys, abs_rel=abs_rel)
        return combined

    def query_all_between(
        self, _query: dict, abs_rel: str = "absolute",
    ):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return []
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)

        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    min_epoch = self.min_score(_hash, pipe=pipe)
                    max_epoch = self.max_score(_hash, pipe=pipe)
                    if count == 0:
                        return []

                    keys = pipe.zrangebyscore(
                        _current_key, min=min_epoch, max=max_epoch, withscores=True
                    )
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        combined = self.helpers.combined_abs_rel(keys, abs_rel=abs_rel)
        return combined

    def query_latest_by_time(
        self, _query, max_epoch, abs_rel="absolute", limit: int = 10
    ):
        """ Get the closest item to a given timestamp. Simply pass in an epoch then watch things fly"""
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return []
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return []
                    keys = pipe.zrangebyscore(
                        _current_key,
                        min=max_epoch,
                        max="+inf",
                        start=0,
                        num=1,
                        withscores=True,
                    )
                    combined = self.helpers.combined_abs_rel(keys, abs_rel)
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        if len(combined) == 0:
            return {}
        return combined[0]

    def query_before(self, _query, max_epoch, abs_rel="absolute"):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return []
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_current_key)
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return []
                    keys = pipe.zrangebyscore(
                        _current_key, min="-inf", max=max_epoch, withscores=True
                    )
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue
        combined = self.helpers.combined_abs_rel(keys, abs_rel)
        return combined

    def query_after(self, _query, min_epoch, abs_rel="absolute"):
        if not self.helpers.validate_query(_query) or abs_rel not in [
            "absolute",
            "relative",
        ]:
            return []
        _hash = self.helpers.generate_hash(_query)
        _current_key = self.helpers.dynamic_key(_hash, abs_rel)
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    count = self.count(_hash, pipe=pipe)
                    if count == 0:
                        return []
                    keys = pipe.zrangebyscore(
                        _current_key, min=min_epoch, max="+inf", withscores=True
                    )
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

        combined = self.helpers.combined_abs_rel(keys, abs_rel)
        return combined

    """
        Other Functions
    """

    def _reset_count(self, query: dict, mongo_data: List[Dict]):
        """ Reset the count for the current mongodb query. We do this by adding records in mongo back into redis. """
        if len(mongo_data) == 0:
            return

        _hash = self.helpers.generate_hash(query)
        phindex = self.connection.incr("placeholder:index")
        delindex = self.connection.incr("deletion:index")
        _hash_key = f"{_hash}:list"
        _hash_placeholder = f"{_hash}:{phindex}"
        _hash_del = f"{_hash}:{delindex}"

        serialized_mongo = [
            orjson.dumps(mon, option=orjson.OPT_SERIALIZE_NUMPY) for mon in mongo_data
        ]
        # rlock = f"{_hash}:lock"
        with self.connection.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(_hash)
                    pipe.watch(_hash_placeholder)
                    pipe.watch(_hash_key)
                    pipe.connection.rpush(_hash_placeholder, *serialized_mongo)
                    pipe.connection.rename(_hash_key, _hash_del)
                    pipe.connection.rename(_hash_placeholder, _hash_key)
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

        self._delete_all(_hash_del)

    def reset(self, query, mongo_data: List[Dict]):
        if not self.helpers.validate_query(query):
            return
        self.pool.schedule(self._reset_count, args=(query, mongo_data))

    def count(self, _hash: str, pipe=None) -> int:
        # ZCARD to get the length of the zset
        _count_hash = f"{_hash}:alist"
        if pipe is not None:
            pipe.watch(_count_hash)
            count = pipe.zcard(_count_hash)
        else:
            count = self.connection.zcard(_count_hash)
        return int(count)

    def general_lock(self, query: dict):
        """ 
            Use this lock to wrap around step functions. 
            It would prevent race conditions on the same object (portfolio manipulation)
        """
        _hash = self.helpers.generate_hash(query)
        general_key = f"{_hash}:lock:generalized"
        return self.connection.lock(general_key)

    def min_score(self, _hash: str, pipe: Optional[Pipeline] = None):
        """max_score Get Max Score

            Get the max score given a key. We get the maximum score and cache it. 
            If we change any information we can swap the cache out dynamically to increase access speed.
        """
        _count_hash = f"{_hash}:rlist"
        if pipe is not None:
            pipe.watch(_count_hash)
            count = pipe.zrangebyscore(
                start=0,
                num=1,
                name=_count_hash,
                min="-inf",
                max="+inf",
                withscores=True,
            )

        else:
            count = self.connection.zrangebyscore(
                name=_count_hash,
                start=0,
                num=1,
                min="-inf",
                max="+inf",
                withscores=True,
            )
        if count is None:
            return float(0.0)

        if len(count) > 0:
            return float(count[0][1])
        return float(0.0)

    # def max_score(self, _hash: str, pipe: Optional[Pipeline] = None):
    #     """max_score Get Max Score

    #         Get the max score given a key. We get the maximum score and cache it.
    #         If we change any information we can swap the cache out dynamically to increase access speed.
    #     """
    #     _count_hash = f"{_hash}:rlist"
    #     if pipe is not None:
    #         pipe.watch(_count_hash)
    #         count = pipe.zrangebyscore(
    #             start=0, num=1,
    #             name=_count_hash,
    #             min="-inf", max="+inf",
    #             withscores=True
    #         )

    #     else:
    #         count = self.connection.zrangebyscore(
    #             name=_count_hash,
    #             start=0, num=1,
    #             min="-inf", max="+inf",
    #             withscores=True
    #         )
    #     if count is None:
    #         return float(0.0)

    #     if len(count) > 0:
    #         return float(count[0][1])
    #     return float(0.0)

    def max_score(self, _hash: str, pipe: Optional[Pipeline] = None):
        """min_score Get Minimum Score

            Get the min score given a key. We get the maximum score and cache it. 
            If we change any information we can swap the cache out dynamically to increase access speed.
        """
        _count_hash = f"{_hash}:alist"
        first_dict: Optional[Dict[AnyStr, Any]] = None

        if pipe is not None:
            pipe.watch(_count_hash)
            first_dict = pipe.zrangebyscore(
                name=_count_hash,
                start=0,
                num=1,
                min="+inf",
                max="-inf",
                withscores=True,
            )
        else:
            first_dict = self.connection.zrangebyscore(
                name=_count_hash,
                start=0,
                num=1,
                min="+inf",
                max="-inf",
                withscores=True,
            )

        if first_dict is None:
            return float(0.0)

        if len(first_dict) > 0:
            return float(first_dict[0][1])
        return float(0.0)
