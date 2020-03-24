import maya
import version_query
from version_query import VersionComponent 
from jamboree.storage.files.redisify import RedisFileProcessor


class RedisSynchronizer(RedisFileProcessor):
    """
        Runs all of the logic about the given object:
        
        Determines if we should save
        Determines how we should save
        
    """


    def add(self, qhash, version, pickled_file):
        vkey = f"{qhash}:{version}"
        self.conn.set(vkey, pickled_file)
    
    def load(self, qhash:str, version:str):
        vkey = f"{qhash}:{version}"
        is_exist = bool(self.conn.exists(vkey))
        if is_exist == False:
            raise AttributeError("File doesn't exist")
        pickled_file = self.conn.get(vkey)
        return pickled_file
    
    """This will """
    def reset(self):
        super().reset()
        print("Reset all syncing information")