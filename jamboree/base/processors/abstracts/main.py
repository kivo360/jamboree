from abc import ABC
from typing import List, Optional
from jamboree.base.processors.abstracts import EventProcessor, FileProcessor, SearchProcessor


class Processor(ABC):
    """ Use to allow for multiple items"""
    def __init__(self):
        self._event:Optional[EventProcessor] = None
        self._storage:Optional[FileProcessor] = None
        self._search:Optional[SearchProcessor] = None

    @property
    def event(self) -> EventProcessor:
        if not isinstance(self._event, EventProcessor):
            raise AttributeError("EventProcessor not added yet ... ")
        return self._event
    
    @event.setter
    def event(self, _event:EventProcessor) -> EventProcessor:
        self._event = _event
    

    @property
    def storage(self) -> FileProcessor:
        if not isinstance(self._storage, FileProcessor):
            raise AttributeError("FileProcessor not added yet ... ")
        return self._storage
    
    @storage.setter
    def storage(self, _storage:FileProcessor):
        self._storage = _storage

    @property
    def search(self) -> SearchProcessor:
        if not isinstance(self._search, SearchProcessor):
            raise AttributeError("SearchProcessor not added yet ... ")
        return self._search
    
    def search(self, _search:SearchProcessor):
        self._search = self.search