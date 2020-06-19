import uuid
from typing import Any, AnyStr, Dict

import maya
import pandas as pd
import ujson
from jamboree import Jamboree, JamboreeNew
from jamboree.handlers.abstracted.search import MetadataSearchHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.handlers.default.db import DBHandler
from jamboree.handlers.default.time import TimeHandler
from jamboree.handlers.processors import (DataProcessorsAbstract,
                                          DynamicResample)
from jamboree.utils import omit
from jamboree.utils.support.search import querying
from loguru import logger


class Access(DBHandler):
    # ---------------------------------------------------------------------------------
    #                          Simple Accessor Properties
    # ---------------------------------------------------------------------------------
    
    @property
    def name(self):
        return self['name']

    @name.setter
    def name(self, __name: str):
        self['name'] = __name

    
    @property
    def category(self) -> str:
        return self['category']

    @category.setter
    def category(self, _category: str):
        self['category'] = _category

    @property
    def subcategories(self) -> str:
        return self['subcategories']

    @subcategories.setter
    def subcategories(self, __subcatgeories: Dict[AnyStr, Any]):
        self['subcategories'] = __subcatgeories

    @property
    def metatype(self) -> str:
        return self['metatype']

    @metatype.setter
    def metatype(self, __metatype: str):
        self['metatype'] = __metatype

    @property
    def submetatype(self) -> str:
        return self['submetatype']

    @submetatype.setter
    def submetatype(self, __submetatype: str):
        self['submetatype'] = __submetatype

    @property
    def abbreviation(self) -> str:
        return self['abbreviation']

    @abbreviation.setter
    def abbreviation(self, __abb: str):
        self['abbreviation'] = __abb