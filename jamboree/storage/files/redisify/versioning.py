import maya
import version_query
from version_query import VersionComponent 
from jamboree.storage.files.redisify import RedisFileProcessor



class RedisVersioning(RedisFileProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._current_checksum = None
        self._query_hash = None
        self._current_version = None
        self.is_update = False
        

    
    @property
    def vkey(self):
        version_key = f"{self.qhash}:version_set"
        return version_key
    
    @property
    def version(self):
        if self._current_version is None:
            self._current_version = self.latest
        return self._current_version
    
    @property
    def qhash(self):
        if self._query_hash is None:
            raise AttributeError("Checksum hasn't been loaded yet")
        return self._query_hash
    
    @qhash.setter
    def qhash(self, qhash:str):
        self._query_hash = qhash
    
    
    @property
    def exist(self) -> bool:
        is_exist = bool(self.conn.exists(self.vkey))
        version_count = self.conn.zcard(self.vkey)
        above_zero = (version_count >= 0)
        if is_exist and above_zero:
            self.is_update = True
            return True
        return False
    
    @property
    def latest(self):
        """Get the latest version given a hash"""
        if self.exist is False:
            self.conn.zadd(self.vkey, {"0.0.1": maya.now()._epoch})
            return "0.0.1"
        return (self.conn.zrange(self.vkey, -1, -1)[0]).decode()
        
    @property
    def updated(self):
        current = self.version
        if self.is_update is False:
            return current
        vs = version_query.Version.from_str(current)
        
        new_vs = vs.increment(VersionComponent.Patch)
        new_vs_str = new_vs.to_str()
        
        self.conn.zadd(self.vkey, {new_vs_str: maya.now()._epoch})
        return new_vs_str
        
    def reset(self):
        super().reset()
        self.is_update = False
        self._current_version = None