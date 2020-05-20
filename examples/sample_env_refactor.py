import time
import maya
import vaex
import uuid
import random
import numpy as np
import pandas as pd
from copy import copy
from crayons import red, magenta, cyan
from loguru import logger
from jamboree import DBHandler
from jamboree import Jamboree
from contextlib import ContextDecorator
from pprint import pprint

class timecontext(ContextDecorator):
    def __enter__(self):
        self.start = maya.now()._epoch
        return self

    def __exit__(self, *exc):
        self.end = maya.now()._epoch
        delta = self.end - self.start
        print(f"It took {delta}ms")
        return False


class SampleEnvHandler(DBHandler):
    """Abstract handler that we use to keep track of information.
    """

    def __init__(self):
        # mongodb_host= "localhost", redis_host="localhost", redis_port=6379
        super().__init__()
        self.entity = "sample"
        self.required = {
            "episode": str
        }
        self._balance = 0
        self._limit = 100
        self['opt_type'] = "live"

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit):
        self._limit = limit

    @property
    def count(self):
        return super().count()

    @property
    def balance(self):
        """ Gets the sum of the last three pop_multiplevalues at set the value """
        return self._balance

    @property
    def transactions(self) -> vaex.dataframe:
        """ Get the last 100 transactions """
        many_records = self.many(self.limit)

        if isinstance(many_records, dict):
            frame = pd.DataFrame(many_records)
            transactions_frame = vaex.from_pandas(frame)
            return transactions_frame.sort('timestamp', ascending=False)

        if len(many_records) > 0:
            frame = pd.DataFrame(many_records)
            transactions_frame = vaex.from_pandas(frame)
            return transactions_frame.sort('timestamp', ascending=False)

        return vaex.from_pandas(pd.DataFrame())

    def save_update_recent(self, data: dict):
        transactions = self.transactions
        count = transactions.count()
        new_value = data['value'] + count
        data['value'] = int(new_value)
        super().save(data)

    def pop_many(self, _limit: int = 1, alt: dict = {}):
        return super().pop_many(_limit, alt)

    def copy(self):
        new_sample = SampleEnvHandler()
        new_sample.data = copy(self.data)
        new_sample.required = copy(self.required)
        new_sample._required = copy(self._required)
        new_sample.limit = copy(self.limit)
        new_sample.event_proc = self.event_proc
        return new_sample


def flip(n=0.02):
    if n >= random.uniform(0, 1):
        return True
    return False


def main():
    jambo = Jamboree()
    sample_env_handler = SampleEnvHandler()
    sample_env_handler.limit = 250
    sample_env_handler.processor = jambo
    # with timecontext():
    current_time = maya.now()._epoch
    mult = 60

    # Create a new set of records and swap to another location to be acted on.
    sample_env_handler['episode'] = uuid.uuid1().hex
    with timecontext():
        super_index = 0
        for _ in range(100):
            v1 = random.uniform(0, 12)
            sample_env_handler.save({"value": v1, "time": (current_time + (mult * super_index))})
            super_index += 1
        
        many_list = []
        catch_index_1 = random.randint(super_index-10, super_index+3)
        catch_index_2 = random.randint(super_index-10, super_index+3)
        last_by_time = (current_time + (mult * catch_index_1))
        last_by_time_2 = (current_time + (mult * catch_index_2))
        for _ in range(10):
            item = {"valuesssssss": random.uniform(0, 12), "time": (current_time + (mult * super_index))}
            many_list.append(item)
            super_index += 1
        
        sample_env_handler.save_many(many_list)
        latest = sample_env_handler.last()
        last_by = sample_env_handler.last_by(last_by_time, ar="relative")
        last_by_2 = sample_env_handler.last_by(last_by_time_2, ar="relative")
        

        t1 = last_by.get('time', time.time())
        t2 = last_by_2.get('time', time.time())
        
        logger.info(latest)
        logger.info(magenta(last_by_time, bold=True))
        logger.success(t1)
        logger.error(t2)
        logger.info(cyan(t1-t2, bold=True))


if __name__ == "__main__":
    main()
