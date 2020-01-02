
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

class UserSettingsHandler(DBHandler):
    """Abstract handler that we use to keep track of information.
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.entity = "user_settings"
        self.required = {
            "email": str,
            "episode": str
        }
        self._limit = 100
        self._settings_handler = None

    @property
    def limit(self):
        """ The maximum number of records we intend to get when calling the many function."""
        return self._limit
    
    @limit.setter
    def limit(self, limit):
        self._limit = limit 
    
    def is_authenticated(self):
        return True
    
    def is_active(self):
        return True
 
    def is_anonymous(self):
        return False

    def register(self, password:str, confirm:str):
        pass

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
    

    def latest_user(self):
        pass

    def save_user(self):
        pass

    

def flip(n=0.02):
    if n >= random.uniform(0, 1):
        return True
    return False