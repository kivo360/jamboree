from abc import ABC
from typing import List

class FileProcessor(ABC):
    """ 
        # File processor abstract. 
        
        Deals with all blobs and files..
    """
    
    def initialize(self):
        pass

    def save(self, query: dict, data: dict, **kwargs):
        """ Save a single blob of data. """
        raise NotImplementedError

    def save_version(self, query, **kwargs):
        """ Save a single blob of data at a given version. """
        raise NotADirectoryError

    def query(self, query, **kwargs):
        """ Query a blob of data. Get the latest """
        raise NotImplementedError

    def query_version(self, query, **kwargs):
        """ Save an explicit version of data """
        raise NotImplementedError

    def delete(self, query, **kwargs):
        """ Delete the latest version of data """
        raise NotImplementedError
    
    def delete_version(self, query:dict, **kwargs):
        """ Delete a given version of data if it exist """
        raise NotImplementedError

    def delete_all(self, query:dict, **kwargs):
        """ Purge everything """
        raise NotImplementedError
    
    def absolute_exists(self, query:dict, **kwargs):
        raise NotImplementedError