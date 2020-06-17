from abc import ABC
from jamboree.utils.helper import Helpers
from pebble.pool import ThreadPool
from multiprocessing import cpu_count
from typing import Union, Optional
from redis import Redis
from redis.client import Pipeline


class DatabaseConnection(ABC):
    def __init__(self) -> None:
        self._connection: Optional[Union[Redis, Pipeline]] = None
        self.helpers = Helpers()
        self._pool = ThreadPool(max_workers=(cpu_count() * 2))

    @property
    def connection(self) -> Union[Redis, Pipeline]:
        if self._connection is None:
            raise AttributeError(
                "You haven't added a main database connection as of yet."
            )
        return self._connection

    @connection.setter
    def connection(self, _conn: Union[Redis, Pipeline]):
        self._connection = _conn

    @property
    def pool(self) -> ThreadPool:
        return self._pool

    @pool.setter
    def pool(self, _pool: ThreadPool):
        self.pool = _pool

    """ Save commands """

    def save(self, query):
        raise NotImplementedError("save not implemented")

    def save_many(self, query):
        raise NotImplementedError("save_many not implemented")

    """
        Update commands
    """

    def update_single(self, query):
        raise NotImplementedError("update_single not implemented")

    def update_many(self, query):
        raise NotImplementedError("update_many not implemented")

    """
        Delete Commands
    """

    def delete(self, query):
        raise NotImplementedError("delete function not implemented yet.")

    def delete_many(self, query):
        raise NotImplementedError("delete_many function not implemented yet.")

    def delete_all(self, query):
        raise NotImplementedError("delete_all not implemented")

    """ 
        Query commands
    """

    def query_latest(self):
        raise NotImplementedError("query_latest not implemented")

    def query_latest_many(self):
        raise NotImplementedError("query_latest_many not implemented")

    def query_between(self):
        raise NotImplementedError("query_between not implemented")

    def query_before(self):
        raise NotImplementedError("query_before not implemented")

    def query_after(self):
        raise NotImplementedError("query_after not implemented")

    def query_all(self):
        pass

    """ Other Functions """

    def reset(self, query):
        raise NotImplementedError("delete_all not implemented")

    def count(self):
        raise NotImplementedError("update_many not implemented")

    def general_lock(self, query: dict):
        raise NotImplementedError("general_lock not implemented")

