import time
import vaex
import uuid
import pandas as pd
import numpy as np
import maya
from jamboree import DBHandler
from jamboree.base.refactor_final import Jamboree
import random
from random import randint
from contextlib import ContextDecorator
# from pprint import pprint
# from crayons import blue, red
# from toolz.itertoolz import pluck
from copy import copy
from loguru import logger


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

    def load_to_backtest(self, alt: dict = {}):
        # Get the current record first

        current_backtest = self.copy()
        current_backtest['opt_type'] = "backtest"
        current_backtest['episode'] = uuid.uuid4().hex
        logger.info(self['episode'])
        logger.info(current_backtest['episode'])

        last_1000 = self.many(limit=self.limit)

        logger.info(len(last_1000))

        current_backtest.save_many(last_1000)
        current_backtest.swap_many(limit=5)
        swapped = current_backtest.query_mix(limit=5)
        logger.info(red(swapped))

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
    sample_env_handler.event = jambo
    sample_env_handler['episode'] = uuid.uuid1().hex
    # with timecontext():
    current_time = maya.now()._epoch
    mult = 60

    # Create a new set of records and swap to another location to be acted on.
    sample_env_handler['episode'] = uuid.uuid1().hex
    with timecontext():
        for _ in range(1000):
            v1 = random.uniform(0, 12)
            sample_env_handler.save({"value": v1, "time": (current_time + (mult * _))})
        latest = sample_env_handler.last()
        logger.info(latest)


if __name__ == "__main__":
    main()
