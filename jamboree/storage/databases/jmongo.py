import maya
from loguru import logger
from typing import Dict, List, Any
from jamboree.storage.databases import DatabaseConnection


class MongoDatabaseConnection(DatabaseConnection):
    def __init__(self) -> None:
        super().__init__()

    """ Save commands """

    def save(self, query: dict, data: dict):
        if not self.helpers.validate_query(query):
            # Log a warning here instead
            return
        timestamp = maya.now()._epoch
        query.update(data)
        query['timestamp'] = timestamp
        self.connection.store(query)

    def save_many(self, query: Dict[str, Any], data: List[Dict]):
        if not self.helpers.validate_query(query) or len(data) == 0:
            return

        first_item = data[0]
        first_item.update(query)
        updated_list = [self.helpers.update_dict(query, x) for x in data]
        self.connection.bulk_upsert(updated_list, _column_first=first_item.keys(), _in=['timestamp'])

    """
        Update commands
    """

    def update_single(self, query):
        pass

    def update_many(self, query):
        pass

    """
        Delete Commands
    """

    def delete(self, query: dict, details: dict):
        if not self.helpers.validate_query(query):
            return

        query.update(details)
        self.connection.delete(query)

    def delete_many(self, query: dict, details: dict = {}):
        if not self.helpers.validate_query(query):
            return

        query.update(details)
        self.connection.delete_many(query)

    def delete_all(self, query: dict):
        logger.info("Same as `delete_many`")
        self.delete_many(query)

    """ 
        Query commands
    """

    def query_latest(self, query: dict, abs_rel="absolute"):
        if not self.helpers.validate_query(query):
            return {}
        latest_items = self.connection.query_last(query)
        return latest_items

    def query_latest_many(self, query: dict):
        if not self.helpers.validate_query(query):
            return []
        latest_items = list(self.connection.query_latest(query))
        return latest_items

    def query_all(self, query: dict):
        if not self.helpers.validate_query(query):
            return []
        mongo_data = list(self.connection.query(query))
        return mongo_data
    

    def query_latest_by_time(self, query:dict, max_epoch:float, abs_rel:str="absolute", limit:int=10):
        if not self.helpers.validate_query(query):
            return {}
        latest_items = self.connection.query_closest(query)
        return latest_items

    def query_between(self, query:dict, min_epoch:float, max_epoch:float, abs_rel:str="absolute"):
        if not self.helpers.validate_query(query):
            return {}
        
        latest_items = list(self.connection.query_time(query, time_type="window", start=min_epoch))
        if len(latest_items) == 0:
            return []
        return latest_items
    
    def query_before(self, query):
        if not self.helpers.validate_query(query):
            return []
        mongo_data = list(self.connection.query(query))
        return mongo_data
    
    def query_after(self, query):
        if not self.helpers.validate_query(query):
            return []
        mongo_data = list(self.connection.query(query))
        return mongo_data

    """ Swap focused commands"""

    def query_mix_swap(self):
        pass

    def swap(self):
        pass

    """ 
        Pop commands
    """

    def pop(self, query: dict):
        if not self.helpers.validate_query(query):
            return []

        query['limit'] = 1
        item = list(self.connection.query_latest(query))
        if item is not None:
            self.connection.delete(item)
        return item

    def pop_many(self, query: dict, limit: int = 10):
        if not self.helpers.validate_query(query):
            return []

        query['limit'] = limit
        items = list(self.connection.query_latest(query))
        if len(items) == 0:
            return []
        for item in items:
            self.connection.delete(item)
        return items

    def get_latest_many_swap(self):
        pass

    """ Other Functions """

    def reset(self, query: dict):
        pass

    def count(self, query: dict):
        if not self.helpers.validate_query(query):
            return 0
        query.pop('limit', None)
        records = list(self.connection.query(query))
        record_len = len(records)
        return record_len
