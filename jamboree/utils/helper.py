"""
    A class that holds all of the helper functions.
"""
import base64
import uuid
from abc import ABC
from copy import copy
from typing import Any, Dict, List, Set

import maya
import orjson
import pandas as pd
import ujson


class Helpers(object):
    def __init__(self) -> None:
        pass

    def generate_hash(self, query: dict) -> str:
        _hash = ujson.dumps(query, sort_keys=True)
        _hash = base64.b64encode(str.encode(_hash))
        _hash = _hash.decode("utf-8")
        return _hash

    def hash_to_dict(self, _hash: str) -> str:
        # TODO: Convert Hash To Dict
        # NOTE: Not tested. Please make sure to test it
        __hash = base64.b64decode(_hash).decode("utf-8")
        _hash_json = ujson.loads(__hash)
        return _hash_json

    def add_time(self, item: dict, _time: float, rel_abs="absolute"):
        if rel_abs == "absolute":
            item["timestamp"] = _time
        else:
            item["time"] = _time
        return item

    def convert_to_storable_json(self, json_string: str):

        savable = {}
        for key, value in orjson.loads(json_string).items():
            item_json = orjson.dumps(value)
            timestamp = float(key) * 0.001
            savable[item_json] = timestamp
        return savable

    def convert_to_storable(self, items: list):
        savable = {}
        for item in items:
            timestamp = float(item.pop("timestamp", maya.now()._epoch))
            item_json = orjson.dumps(item)
            savable[item_json] = timestamp

        return savable

    def convert_to_storable_abs(self, items: list):
        savable = {}
        for item in items:
            timestamp = float(item.pop("timestamp", maya.now()._epoch))
            item_json = orjson.dumps(item)
            savable[item_json] = timestamp

        return savable

    def convert_to_storable_relative(self, items: list):
        savable = {}
        for item in items:
            timestamp = float(item.pop("time", maya.now()._epoch))
            item_json = orjson.dumps(item)
            savable[item_json] = timestamp

        return savable
    def dual_storable(self, items: list):
        relative = self.convert_to_storable_relative(items)
        absolute = self.convert_to_storable_abs(items)
        return {"relative": relative, "absolute": absolute}

    def convert_dataframe_to_storable_item(self, df: pd.DataFrame) -> dict:
        data = df.astype(str)
        data_json = data.to_json(orient="index")
        value = self.convert_to_storable_json(data_json)
        return value

    def get_current_abs_time(self, data: dict):
        _data = copy(data)
        for k, v in data.items():
            _data[k] = maya.now()._epoch
        return _data

    def generate_dicts(self, data: dict, _time: float, timestamp: float):
        relative = copy(data)
        absolute = copy(data)
        relative["time"] = _time
        absolute["timestamp"] = _time
        return {"relative": relative, "absolute": absolute}

    def is_abs_rel(self, query_type: str):
        if query_type in ["absolute", "relative"]:
            return True

        return False

    def is_zero_time(self, item: Dict[str, Any]) -> bool:
        """ 
            Check to see if any of the items has a time of zero:

            **Parameters:**
                - items: List items that represent a sequence of dictionaries from the data store.
            **Response:**
                - Is false if there are errors in the timestamp. Or if time or timestamp is 0
                - return Boolean
        """
        if "time" not in item and "timestamp" not in item:
            return False

        if "time" in item and "timestamp" in item:
            if item["time"] == 0 or item["timestamp"] == 0:
                return False
        if "time" in item:
            if item["time"] == 0:
                return False
        if "timestamp" in item:
            if item["timestamp"] == 0:
                return False
        return True

    def dictify(self, azset: List[Set], rzset: List[Set]):
        """Create a dictionary"""
        if len(azset) == 0 or len(rzset) == 0:
            return {}
        adict = {}
        for azs in azset:
            item, time = azs
            current_item = adict.get(item, {})
            current_item["timestamp"] = time
            adict[item] = current_item

        for rzs in rzset:
            item, time = rzs
            current_item = adict.get(item, {})
            current_item["time"] = time
            adict[item] = current_item

        return adict

    def add_event_id(self, event: dict):
        event["event_id"] = uuid.uuid4().hex
        return event

    def add_event_ids(self, data: List[Dict[str, Any]]):
        """ Add event ids to a list of events. Use for a save many query. """
        events = [self.add_event_id(x) for x in data]
        return events

    def check_time(
        self,
        _time: float = None,
        _timestamp: float = None,
        local_time: float = None,
        local_timestamp: float = None,
    ):
        current_time = maya.now()._epoch

        if local_time is not None:
            _time = local_time
        elif _time is None:
            _time = current_time
        if local_timestamp is not None:
            _timestamp = local_timestamp
        elif _timestamp is None:
            _timestamp = current_time

        return {"time": _time, "timestamp": _timestamp}

    def deserialize_dicts(self, dictified: dict):
        _deserialized = []
        for key, value in dictified.items():
            _key = orjson.loads(key)
            _key["time"] = value.get("time", maya.now()._epoch)
            _key["timestamp"] = value.get("timestamp", maya.now()._epoch)
            _deserialized.append(_key)
        return _deserialized

    def separate_time_data(
        self, data: dict, _time: float = None, _timestamp: float = None
    ):
        local_time = data.pop("time", None)
        local_timestamp = data.pop("timestamp", None)
        timing = self.check_time(_time, _timestamp, local_time, local_timestamp)
        return data, timing

    def validate_query(self, query: dict):
        """ Validates a query. Must have `type` and a second identifier at least"""
        if "type" not in query:
            return False
        if not isinstance(query["type"], str):
            return False
        if len(query) < 2:
            return False
        return True

    def update_dict(self, query: dict, data: dict):
        query = copy(query)
        timestamp = maya.now()._epoch
        query["timestamp"] = timestamp
        data.update(query)
        return data

    def update_dict_no_timestamp(self, query: dict, data: dict):
        query = copy(query)
        data = copy(data)
        data.update(query)
        data.pop("timestamp", None)
        return data

    def back_to_dict(self, list_of_serialized: list):
        deserialized = []
        if len(list_of_serialized) == 1:
            return orjson.loads(list_of_serialized[0])

        for i in list_of_serialized:
            deserialized.append(orjson.loads(i))
        return deserialized

    def search_one(self, item: dict, query: dict):
        all_bools = []
        for q in query:
            if q in item:
                if query[q] == query[q]:
                    all_bools.append(True)
                else:
                    all_bools.append(False)
            else:
                all_bools.append(False)
        return any(all_bools)

    def dynamic_key(self, _hash, absrel):
        """ Get the absolute key. """
        current_key = ""
        if absrel == "absolute":
            current_key = f"{_hash}:alist"
        else:
            current_key = f"{_hash}:rlist"
        return current_key

    def combine_results(self, akeys, rkeys):
        dicts = self.dictify(akeys, rkeys)
        dicts.pop(b'{"placeholder": "place"}', None)
        combined = self.deserialize_dicts(dicts)
        return combined

    def combined_abs_rel(self, keys, abs_rel="absolute"):
        blank_keys = [(b'{"placeholder": "place"}', 0)]
        dicts = []
        if abs_rel == "absolute":
            dicts = self.dictify(keys, blank_keys)
        else:

            dicts = self.dictify(blank_keys, keys)
        dicts.pop(b'{"placeholder": "place"}', None)
        combined = self.deserialize_dicts(dicts)
        return combined

    def convert_to_storable_json_list(self, json_string) -> list:
        converted_list = []
        for key, value in orjson.loads(json_string).items():
            item_json = value
            item_json["time"] = float(key) * 0.001
            converted_list.append(item_json)
        return converted_list

    def convert_to_storable_no_json(self, json_string: str) -> list:
        converted_list = []
        for key, value in orjson.loads(json_string).items():
            item_json = value
            item_json["time"] = float(key) * 0.001
            converted_list.append(item_json)
        return converted_list

    def convert_dataframe_to_storable_item_list(self, df: pd.DataFrame) -> list:
        data_json = df.to_json(orient="index")
        value = self.convert_to_storable_no_json(data_json)
        return value

    def standardize_record(self, record):
        closing_record = copy(record)
        if "Close" in record:
            closing_record.pop("Close", None)
            closing_record["close"] = record["Close"]
        if "Open" in record:
            closing_record.pop("Open", None)
            closing_record["open"] = record["Open"]
        if "Low" in record:
            closing_record.pop("Low", None)
            closing_record["low"] = record["Low"]
        if "High" in record:
            closing_record.pop("High", None)
            closing_record["high"] = record["High"]
        if "Volume" in record:
            closing_record.pop("Volume", None)
            closing_record["volume"] = record["Volume"]
        if "Adj Close" in record:
            closing_record.pop("Adj Close", None)
            closing_record["adj_close"] = record["Adj Close"]
        return closing_record

    def standardize_outputs(self, records: List[Dict[str, Any]]):
        if len(records) == 0:
            return []
        _records = [self.generic_standardize(rec) for rec in records]
        return _records

    def generic_standardize(self, record: dict):
        if bool(record) == False:
            return {}

        _record = {}
        for k, v in record.items():
            # Normalize
            k = str(k)
            k = k.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?|`~-=_+"})
            k = k.lower()
            k = k.replace(" ", "_")
            _record[k] = v
        return _record
