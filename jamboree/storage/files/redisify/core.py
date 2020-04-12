import maya
from threading import local
from jamboree.storage.files import FileStorageConnection
from jamboree.utils.core import consistent_hash
from jamboree.utils.support.storage import serialize, deserialize
from jamboree.utils.context import watch_loop
from addict import Dict
import redis
from redis import Redis
from redis.client import Pipeline
import version_query
from loguru import logger
logger.disable(__name__)

class RedisFileProcessor(object):
    def __init__(self, *args, **kwargs):
        self._pipe = None
        self._conn = None
    
    @property
    def conn(self) -> Redis:
        if self._conn is None:
            raise AttributeError("Pipe hasn't been set")
        return self._conn
    @conn.setter
    def conn(self, _pipe:Redis):
        self._conn = _pipe
    
    @property
    def pipe(self) -> Pipeline:
        if self._pipe is None:
            raise AttributeError("Pipe hasn't been set")
        return self._pipe
    @pipe.setter
    def pipe(self, _pipe:Pipeline):
        self._pipe = _pipe
    
    
    def reset(self):
        self.pipe = None
        self.conn = None


class RedisFileConnection(FileStorageConnection):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        """ NOTE: Expiriment with sorted sets """
        self.current_query = {}
        self.current_hash = None
        self.current_query_exist = None
        self.current_pipe = None
        self.current_hash_keys = None
        self.current_version = None
        self.current_version_exist = None
        self.setup_run = None

    def gwatch(self):
        sorted_version = self.keys.version.sorted
        set_version = self.keys.version.set
        self.pipe.watch(sorted_version)
        self.pipe.watch(set_version)

    @property
    def version(self):
        """ Get the latest version or the default"""
        sorted_version = self.keys.version.sorted
        set_version = self.keys.version.set
        # self.pipe.watch(sorted_version)
        # self.pipe.watch(set_version)
        if self.query_exists and self.current_version is None:
            # latest_version = self.connection.zrange(sorted_version, -1, -1)
            # _all_versions = self.connection.zrange(sorted_version, 0, -1)
            latest_version = self.pipe.zrange(sorted_version, -1, -1)
            
            if latest_version is not None and len(latest_version) > 0:
                latest_version = latest_version[0]
                self.current_version = latest_version.decode()
        elif self.current_version is not None:
            return self.current_version
        else:
            latest_version = self.settings.default.version
            self.pipe.zadd(sorted_version, {latest_version: maya.now()._epoch})
            self.pipe.sadd(set_version, latest_version)
            self.current_version = latest_version
        return self.current_version
    
    @version.setter
    def version(self, _version:str):
        sorted_version = self.keys.version.sorted
        set_version = self.keys.version.set
        self.pipe.zadd(sorted_version, {_version: maya.now()._epoch})
        self.pipe.sadd(set_version, *_version)
        self.current_version = _version
    


    @property
    def hash_query(self):
        if self.current_hash is None:
            self.current_hash = consistent_hash(self.current_query)
        return self.current_hash
    
    @property
    def keys(self):
        """ Set all of the required keys to something that fits in memory. """
        if self.current_hash_keys is None:
            self.current_hash_keys = Dict()
            self.current_hash_keys.version.set = f"{self.hash_query}:versions"
            self.current_hash_keys.version.sorted = f"{self.hash_query}:zversions"
            self.current_hash_keys.file.sum = f"{self.hash_query}:sums"
            self.current_hash_keys.sum = f"{self.hash_query}:sum"
            self.current_hash_keys.version.index = f"{self.hash_query}:incr"
        return self.current_hash_keys
    
    @property
    def query_exists(self) -> bool:
        if self.current_query_exist is None:
            version_set_exist = self.pipe.exists(self.keys.version.set)
            sorted_version_exist = self.pipe.exists(self.keys.version.sorted)

            self.current_query_exist = (version_set_exist == 1 and sorted_version_exist == 1)
        return self.current_query_exist
    
    @property
    def file_exist(self) -> bool:
        """ Does the current file version exist"""
        
        if self.current_version_exist is None:
            vk = self.version_key
            self.pipe.watch(vk)
            version_set_exist = self.pipe.exists(vk)
            self.current_version_exist = (version_set_exist == 1)
        return self.current_version_exist
    
    @property
    def pipe(self):
        if self.current_pipe is None:
            raise AttributeError("Pipe cannot be non-existent")
        return self.current_pipe

    @property
    def version_key(self) -> str:
        return f"{self.hash_query}:{self.version}"
    

    def update_version(self):
        """ Save version in multiple places to be found later"""
        version =  self.version
        if self.query_exists and not self.is_overwrite:
            vs = version_query.Version.from_str(version)
            new_vs = vs.increment(self.settings.default.increment)
            new_vs_str = new_vs.to_str()
            # print(new_vs_str)
            self.version = new_vs_str
        
    
    def update_file(self, _file):
        """Update the file"""
        vkey = self.version_key
        self.pipe.set(vkey, _file)

    def update(self, file):
        """ Update the file version and update the file. """
        self.update_version()
        self.update_file(file)
        logger.error(file)

    @property
    def garbage_patch(self):
        """ 
            Basically a way to see if both the file and query key exist. 
            It's pretty janky. It'll be fine though. 
        """
        return (self.query_exists and self.file_exist)

    def absolute_exists(self, query, **kwargs):
        # self.setup_run = None
        self.setup(query, **kwargs)
        return (self.query_exists and self.file_exist)


    @logger.catch
    def save(self, query:dict, obj, **kwargs):
        self.setup(query, **kwargs)
        serial_item = serialize(obj)
        self.update(serial_item)
        
        
    

    @logger.catch
    def query(self, query:dict, **kwargs):
        self.setup(query, **kwargs)
        
        if self.garbage_patch:
            # If the query and file exist
            # logger.debug("File exist, we're gonna try pulling it")
            # logger.debug(self.version)
            item = self.pipe.get(self.version_key)
            unpacked = deserialize(item)
            if unpacked is None:
                raise AttributeError("Pickled Item Not Found")
            return unpacked


    def delete(self, query:dict, **kwargs):
        self.setup(query, **kwargs)
        if self.query_exists and self.file_exist:
            # If the query and file exist
            sorted_version = self.keys.version.sorted
            set_version = self.keys.version.set
            self.pipe.delete(self.version_key)
            self.pipe.zrem(sorted_version, self.version)
            self.pipe.srem(set_version, self.version)
    
    def setup(self, query:dict, **kwargs):
        is_force = kwargs.pop("is_force", False)
        if self.setup_run is None or is_force:
            self.reset()
            self.settings = Dict(**kwargs)
            self.current_query = query
            self.gwatch()
            self.version
            self.setup_run = True
            self.file_exist
    
    def reset(self):
        """ Reset all placeholder variables"""
        self.current_query = {}
        self.current_hash = None
        self.current_query_exist = None
        self.current_pipe = self.conn.pipeline()
        self.current_version = None
        self.current_version_exist = None
        self.setup_run = None



class SampleObj(object):
    def __init__(self) -> None:
        self.one = "one"
        self.two = "two"

def main():
    current_settings = Dict()
    query_dict = Dict()
    current_settings.overwrite = True
    current_settings.preferences = query_dict
    samp_opt = SampleObj()
    redpill = redis.Redis()
    redconn = RedisFileConnection()
    redconn.conn = redpill
    redconn.save({"one": "twoss"}, samp_opt, **current_settings)
    item = redconn.query({"one": "twoss"})
    logger.info(item)

    # redconn.pipe.execute()

if __name__ == "__main__":
    main()