from abc import ABC
from jamboree.utils.helper import Helpers
from pebble.pool import ThreadPool
from multiprocessing import cpu_count


class DatabaseConnection(ABC):
    def __init__(self) -> None:
        self._connection = None
        self.helpers = Helpers()
        self._pool = ThreadPool(max_workers=(cpu_count() * 2))

    @property
    def connection(self):
        if self._connection is None:
            raise AttributeError("You haven't added a main database connection as of yet.")
        return self._connection

    @connection.setter
    def connection(self, _conn):
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

    """ Swap focused commands"""

    def query_mix_swap(self):
        raise NotImplementedError("query_mix_swap not implemented")

    def swap(self):
        raise NotImplementedError("swap not implemented")

    """ 
        Pop commands
    """

    def pop(self):
        raise NotImplementedError("pop not implemented")

    def pop_many(self):
        raise NotImplementedError("pop_many not implemented")

    def get_latest_many_swap(self):
        raise NotImplementedError("get_latest_many_swap not implemented")

    """ Other Functions """

    def reset(self, query):
        raise NotImplementedError("delete_all not implemented")

    def count(self):
        raise NotImplementedError("update_many not implemented")
