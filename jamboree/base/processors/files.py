import redis
from redis import Redis
from typing import Optional, Any
from jamboree.utils.helper import Helpers
from jamboree.storage.files.redisify import RedisFileProcessor, RedisFileConnection
from jamboree.base.processors.abstracts import FileProcessor


class JamboreeFileProcessor(FileProcessor):
    def __init__(self) -> None:
        self._redis:Optional[Redis] = None
        self._redis_conn = RedisFileConnection()
        self.helpers = Helpers()
    
    @property
    def rconn(self) -> redis.client.Redis:
        if self._redis is None:
            raise AttributeError("You've yet to add a redis connection")
        return self._redis
    
    @rconn.setter
    def rconn(self, _redis:redis.client.Redis):
        self._redis = _redis

    @property
    def redis_conn(self) -> RedisFileConnection:
        if self._redis_conn is None:
            raise AttributeError("Redis connection hasn't been set")
        return self._redis_conn
    
    @redis_conn.setter
    def redis_conn(self, _rconn: RedisFileConnection):
        self._redis_conn = _rconn

    def initialize(self):
        """ Initialize database connections. Use this so we can use the same connections for search, files, and events. """
        self.redis_conn = RedisFileConnection()
        self.redis_conn.conn = self.rconn

    def _validate_query(self, query: dict):
        """ Validates a query. Must have `type` and a second identifier at least"""
        if 'type' not in query:
            return False
        if not isinstance(query['type'], str):
            return False
        if len(query) < 2:
            return False
        return True
    

    """ 
        These are the basic functions. We'll create more functions to handle the different scenarios. 

        NOTE:
            * Eventually we'll add rocksdb and a check_local=False flag to explain that we want to search rocksdb first.
            * We can add a changed flag into redis. 
                * If we've changed a model on a different machine we can just say check_local=False
                * Otherwise we can set check_local=True and force retrieval from redis.
                * If we were to do that, we'd have to separate all of the setup functions inside of the redis handler
    """

    
    def save(self, query:dict, obj:Any, **kwargs):
        if not self._validate_query(query):
            raise ValueError("Query isn't valid")
        
        self.redis_conn.save(query, obj, **kwargs)
    
    def query(self, query:dict, **kwargs):
        if not self._validate_query(query):
            raise ValueError("Query isn't valid")
        data = self.redis_conn.query(query, **kwargs)
        return data

    def delete(self, query:dict, **kwargs):
        if not self._validate_query(query):
            raise ValueError("Query isn't valid")
        self.redis_conn.delete(query, **kwargs)

    def absolute_exists(self, query: dict, **kwargs):
        if not self._validate_query(query):
            raise ValueError("Query isn't valid")
        return self.redis_conn.absolute_exists(query, **kwargs)
    
    
    