
from examples.sample_env import main
import time
import vaex
import uuid
import pandas as pd
import numpy as np
import maya
from jamboree import Jamboree, DBHandler
import random
from random import randint
from contextlib import ContextDecorator
from pprint import pprint
from crayons import blue
from toolz.itertoolz import pluck
from copy import copy
from loguru import logger

class UserHandler(DBHandler):
    """Abstract handler that we use to keep track of information.
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.entity = "user"
        self.required = {
            "user_id": str
        }
        self._balance = 0
        self._limit = 500
        self._settings_handler = None

    @property
    def limit(self):
        """ The maximum number of records we intend to get when calling the many function."""
        return self._limit
    
    @limit.setter
    def limit(self, limit):
        self._limit = limit 
    
    @property
    def settings(self):
        if self._settings_handler is None:
            raise AttributeError
        return self._settings_handler
    
    @settings.setter
    def settings(self, _settings):
        self._settings_handler = _settings
        self._settings_handler.limit = self.limit

    @property
    def balance(self):
        """ Gets the sum of the last three values at set the value """
        return self._balance

    def is_authenticated(self):
        return True
    
    def is_active(self):
        return True
 
    def is_anonymous(self):
        return False

    def _check_password_register(self, password:str, confirm:str):
        """ Run through a set of password conditions"""
        return password == confirm

    def register(self, password:str, confirm:str, first:str, middle:str, last:str):
        first = str.capitalize(first)
        middle = str.capitalize(middle)
        last = str.capitalize(last)

        is_match = self._check_password_register(password, confirm)
        if is_match:
            logger.debug("Passwords are valid")


    def login(self, password:str):
        pass

    def logout(self):
        pass

    def session(self):
        pass

    
    def deactivate(self):
        pass

    def reactivate(self):
        pass
    

    # --------------------------------------------------------
    # --------------------- Counting -------------------------
    # --------------------------------------------------------

    # Use to get counts inside of the database


    def user_record_count(self) -> int:
        count = self.count()
        return count
    


    # --------------------------------------------------------
    # --------------------- Querying -------------------------
    # --------------------------------------------------------

    def latest_user(self):
        """ Get the latest user record """
        last_state = self.last()
        return last_state
    

    def many_user(self):
        latest_user_records = self.many(self.limit)
        return latest_user_records

    
    # --------------------------------------------------------
    # ----------------------- Saving -------------------------
    # --------------------------------------------------------

    def save_user(self, data:dict):
        query = copy.copy(self._query)
        query.update(data)
        query['time'] = maya.now()._epoch
        query['type'] = self.entity
        query['timestamp'] = maya.now()._epoch
        self.save(data)
    

def flip(n=0.02):
    if n >= random.uniform(0, 1):
        return True
    return False

if __name__ == "__main__":
    user_handler = UserHandler()

    user_handler.register("password1", "password1", "kevin", "andrew", "hill")