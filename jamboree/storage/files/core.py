from copy import copy
from abc import ABC
from typing import Any, List
from addict import Dict
from version_query import VersionComponent
import hashlib 

class FileStorageConnection(ABC):
    def __init__(self, **kwargs) -> None:
        self._connection = None
        self._settings = Dict()
        self._settings.overwrite = False
        self._settings.sig_key = kwargs.get("signature", "basic_key")
        
        self._settings.preferences.by = "latest"
        self._settings.preferences.limit = 500
        self._settings.preferences.version = None
        self._settings.default.version = "0.0.1"
        self._settings.default.increment = VersionComponent.Patch
        



    @property
    def conn(self):
        if self._connection is None:
            raise AttributeError("You haven't added a main database connection as of yet.")
        return self._connection

    @conn.setter
    def conn(self, _conn):
        self._connection = _conn

    @property
    def settings(self):
        return self._settings

    @settings.setter
    def settings(self, _settings:Dict):
        copied = self._settings
        copied.update(_settings)
        self.valid_settings(copied)
        self._settings = copied

    def valid_settings(self, _settings):
        if self._settings.preferences.by not in ["latest", "many", "all", "version"]:
            raise ValueError("We must query within a given range of types: 'latest', 'all', 'version'")
        
        if self._settings.preferences.by == "version" and self._settings.preferences.version is None:
            raise AttributeError("If you're querying by version, you have to include a version number (string_format)")

    @property
    def is_overwrite(self) -> bool:
        return self._settings.overwrite


    """ Main Commands """

    def save(self, query:dict, obj:Any, **kwargs):
        raise NotImplementedError("save not implemented")
    
    def query(self, query, **kwargs):
        raise NotImplementedError("query not implemented")
    
    def delete(self, query, **kwargs):
        raise NotImplementedError("delete_latest not implemented")

    def absolute_exists(self, query, **kwargs):
        raise NotImplementedError