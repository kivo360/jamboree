from typing import Dict, List
from copy import copy

import maya
import orjson
from loguru import logger

from jamboree.storage.databases import DatabaseConnection


class RedisDatabaseConnection(DatabaseConnection):
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
        Save commands
    """

    def _save(self, _hash: str, data: dict):
        serialized = orjson.dumps(data)
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            push_key = f"{_hash}:list"
            self.connection.rpush(push_key, serialized)

    def save(self, query: dict, data: dict):
        if not self.helpers.validate_query(query):
            return
        timestamp = maya.now()._epoch
        _hash = self.helpers.generate_hash(query)
        query.update(data)
        query['timestamp'] = timestamp
        self._save(_hash, query)

    def _save_many(self, _hash: str, data: List[Dict]):
        serialized_list = [orjson.dumps(x) for x in data]

        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            push_key = f"{_hash}:list"
            self.connection.rpush(push_key, *serialized_list)

    def save_many(self, query, data: List[Dict] = []):
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        updated_list = [self.helpers.update_dict(query, x) for x in data]
        _hash = self.helpers.generate_hash(query)
        self._save_many(_hash, updated_list)

    """
        Update commands
    """

    def _update_single(self, _hash: str, data: dict):
        serialized = orjson.dumps(data)
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            push_key = f"{_hash}:list"
            self.connection.rpush(push_key, serialized)

    def update_single(self, query: dict, data: dict):
        if not self.helpers.validate_query(query):
            return
        _hash = self.helpers.generate_hash(query)
        self._update_single(_hash, data)

    def _update_many(self, _hash: str, data: List[Dict] = []):
        serialized_list = [orjson.dumps(x) for x in data]

        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            push_key = f"{_hash}:list"
            self.connection.rpush(push_key, *serialized_list)

    def update_many(self, query, data: List[Dict] = []):
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        updated_list = [self.connection.update_dict(query, x) for x in data]
        _hash = self.helpers.generate_hash(query)
        self._update_many(_hash, updated_list)

    """
        # Delete Commands
        ---
        
    """

    def delete_first(self, query, details):
        pass

    def _delete(self, _hash: str, details: dict):
        phindex = self.connection.incr("placeholder_del:index")
        placeholder_hash = f"{_hash}:placeholder:{phindex}"
        placeholder_hash_del = f"{_hash}:placeholder_del:{phindex}"
        push_key = f"{_hash}:list"
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            all_matching_redis_items = self.helpers.back_to_dict(self.connection.lrange(push_key, 0, -1))
            if isinstance(all_matching_redis_items, dict):
                """ Remove replace the current list with the empty one"""
                is_true = self.helpers.search_one(all_matching_redis_items, details)
                if is_true == False: return
                self.connection.rpush(placeholder_hash, orjson.dumps(all_matching_redis_items))
            else:
                for match in all_matching_redis_items:
                    is_true = self.helpers.search_one(match, details)
                    if is_true:
                        self.connection.rpush(placeholder_hash, orjson.dumps(match))

            self.connection.rename(push_key, placeholder_hash_del)
            self.connection.rename(placeholder_hash, push_key)

    def delete(self, query: dict, details: dict):
        if not self.helpers.validate_query(query):
            return
        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return
        self._delete(_hash, details)

    def _delete_many(self, _hash: str, data: List[Dict]):
        pass

    def delete_many(self, query, data: List[Dict]):
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        updated_list = [self.connection.update_dict(query, x) for x in data]
        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return
        self._delete_many(_hash, updated_list)

    def _delete_all(self, _hash: str):
        while self.connection.llen(_hash) > 0:
            self.connection.ltrim(_hash, 0, -99)

    def delete_all(self, query: dict, details: dict):
        if not self.helpers.validate_query(query):
            return

        _hash = self.helpers.generate_hash(query)
        self._delete_all(_hash)

    """ 
        Query commands
    """

    def query_latest(self, query: dict):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []

        latest_redis_items = self.helpers.back_to_dict(self.connection.lrange(f"{_hash}:list", -1, -1))
        return latest_redis_items

    def query_latest_many(self, query: dict, limit=1000):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []

        latest_redis_items = self.helpers.back_to_dict(self.connection.lrange(f"{_hash}:list", -limit, -1))
        return latest_redis_items

    def query_all(self, query: dict):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []

        latest_redis_items = self.helpers.back_to_dict(self.connection.lrange(f"{_hash}:list", 0, -1))
        return latest_redis_items

    """ Swap focused commands"""

    def _latest_many_swap(self, _hash: str, limit: int = 10):
        """ Actually do the redis operation here. """
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            with self.connection.pipeline() as pipe:
                latest_items = []
                try:
                    push_key = f"{_hash}:list"
                    swap_key = f"{_hash}:swap"

                    pipe.watch(push_key)
                    pipe.watch(swap_key)
                    main_count = pipe.llen(push_key)
                    swap_count = pipe.llen(swap_key)

                    if main_count == 0 and swap_count == 0: raise AttributeError("Skip further queries")

                    # Determine the amount we're going to get from the main search
                    limit_swap_diff = limit - swap_count

                    main_req = 0
                    swap_req = 0

                    # ----------------------------------------------------------
                    # -------------- Get the query requirements ----------------
                    # ----------------------------------------------------------

                    # logger.info(main_count)
                    # logger.info(swap_count)
                    if limit_swap_diff < 0:
                        swap_req = limit
                    elif limit_swap_diff >= 1:
                        swap_req = swap_count
                        main_req = limit_swap_diff

                    main_latest_items = []
                    if main_req != 0:
                        main_latest_items = pipe.lrange(push_key, -main_req, -1)

                    swap_latest_items = list(reversed(pipe.lrange(swap_key, -swap_req, -1)))
                    latest_items = main_latest_items + swap_latest_items
                    # means the count of the swapped elements is less than the total limit, yet isn't empty
                    pipe.execute()
                    logger.info(latest_items)
                except Exception as e:
                    logger.error(str(e))
                finally:
                    pipe.reset()

                if len(latest_items) > 0:
                    items = self.helpers.back_to_dict(latest_items)

                    return items
                return latest_items

    def query_mix(self, query: dict, _limit: int):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []
        return self._latest_many_swap(_hash, _limit)

    def query_latest_swap(self, query: dict):
        if not self.helpers.validate_query(query):
            return {}

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return {}
        return self._latest_many_swap(_hash, 1)

    def _swap(self, _hash: str, limit):
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            with self.connection.pipeline() as pipe:
                latest_items = []
                try:
                    push_key = f"{_hash}:list"
                    swap_key = f"{_hash}:swap"
                    pipe.watch(push_key)
                    pipe.watch(swap_key)

                    abs_limit = abs(limit)

                    latest_items = pipe.lrange(push_key, -(abs_limit), -1)
                    latest_items_reversed = copy(latest_items)
                    pipe.ltrim(push_key, 0, -(abs_limit + 1))
                    if len(latest_items) > 0:
                        latest_items_reversed = list(reversed(latest_items_reversed))
                        pipe.rpush(swap_key, *latest_items_reversed)

                    pipe.execute()

                except Exception as e:
                    logger.info(str(e))
                finally:
                    pipe.reset()
                if len(latest_items) > 0:
                    return self.helpers.back_to_dict(latest_items)
                return latest_items

    def swap(self, query: dict, limit: int = 100):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []
        return self._swap(_hash, limit)

    def swap_one(self, query):
        if not self.helpers.validate_query(query):
            return []

        _hash = self.helpers.generate_hash(query)
        count = self.count(_hash)
        if count == 0: return []
        limit = 1
        return self._swap(_hash, limit)

    """ 
        Pop commands
    """

    def _pop_many(self, _hash: str, limit: int = 10):
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            with self.connection.pipeline() as pipe:
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
                    return self.helpers.back_to_dict(latest_items)
                return latest_items

    def pop(self, query: dict, limit=1):
        if not self.helpers.validate_query(query):
            return {}
        _hash = self.helpers.generate_hash(query)
        if self.count(_hash) == 0:
            return {}
        return self._pop_many(_hash, limit=limit)

    def pop_many(self, query: dict, limit=100):
        if not self.helpers.validate_query(query):
            return []
        _hash = self.helpers.generate_hash(query)
        if self.count(_hash) == 0:
            return []
        return self._pop_many(_hash, limit=limit)

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

        serialized_mongo = [orjson.dumps(mon) for mon in mongo_data]
        rlock = f"{_hash}:lock"
        with self.connection.lock(rlock):
            with self.connection.pipeline() as pipe:
                pipe.watch(_hash)
                pipe.watch(_hash_placeholder)
                pipe.watch(_hash_key)
                pipe.connection.rpush(_hash_placeholder, *serialized_mongo)
                pipe.connection.rename(_hash_key, _hash_del)
                pipe.connection.rename(_hash_placeholder, _hash_key)

                pipe.execute()

        self._delete_all(_hash_del)

    def reset(self, query, mongo_data: List[Dict]):
        if not self.helpers.validate_query(query): return
        self.pool.schedule(self._reset_count, args=(query, mongo_data))

    def count(self, _hash: str) -> int:
        _count_hash = f"{_hash}:list"
        count = self.connection.llen(_count_hash)
        return int(count)
