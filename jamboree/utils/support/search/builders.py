from typing import Dict
from loguru import logger
from jamboree.utils.support.search import (
    is_valid_geo, is_valid_bool, is_valid_numeric, is_valid_tags, is_valid_text
)

class QueryBuilder(object):
    """
        # QueryBuilder

        Builds a redis query given a set of builder parameters.

        Some assumptions:
            - All tag queries
    """

    def __init__(self):
        self._query_set = {
            
        }
        self._boolean_fields = set()
        self._number_fields = set()
        self._text_fields = set()
        self._tag_fields = set()
        self._geo_fields = set()

        self.geo_query = {

        }

    @property
    def qset(self) -> dict:
        return self._query_set
    
    
    @property
    def geos(self):
        return self._geo_fields
    
    
    @geos.setter
    def geos(self, _geos:list):
        """ Set all of the Geolocational fields """
        self._geo_fields = set(_geos)
        
    
    def exact(self, field, num):
        placeholder = {
            "filter": "number",
            "value": {
                "upper": num,
                "lower": num
            }
        }
        self._number_fields.add(field)
        self.qset[field] = placeholder
        return self

    def greater(self, field:str, num):
        placeholder = {
            "filter": "number",
            "value": {
                "upper": "+inf",
                "lower": num
            }
        }
        self._number_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    
    def less(self, field, num):
        placeholder = {
            "filter": "number",
            "value": {
                "upper": num,
                "lower": "-inf"
            }
        }
        self._number_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    
    def between(self, field:str, upper:float, lower:float):
        """
            Search a number field for everything between the two points provided
        """

        placeholder = {
            "filter": "number",
            "value": {
                "upper": upper,
                "lower": lower
            }
        }
        self._number_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    
    def near(self, field:str, _long:float, _lat:float, distance:float=1, metric:str="km"):
        """ Get all of the geo fields and generate a query where everything in proximity will be picked up. """
        placeholder = {
            "filter": "geo",
            "value": {
                "long": _long,
                "lat": _lat,
                "distance": distance,
                "metric": metric
            }
        }
        self._geo_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    
    def boolean(self, field:str, is_true=False):
        placeholder = {
            "filter": "boolean"
        }
        if is_true:
            placeholder['value'] = "TRUE"
        else:
            placeholder['value'] = "FALSE"
        self._boolean_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    def tags(self, field, tags:list, op="AND"):
        placeholder = {
            "filter": "tags",
            "operation": op,
            "value": {
                "tags": tags
            }
        }
        self._tag_fields.add(field)
        self.qset[field] = placeholder
        return self
    
    def term(self, field, _term:str, is_exact=False):
        placeholder = {
            "filter": "text",
            "is_exact": is_exact,
            "value": _term
        }
        self._text_fields.add(field)
        self.qset[field] = placeholder
    
    def insert_by_type_str(self, _type_str:str, field:str, value):
        if _type_str == "BOOL":
            self.boolean(field, is_true=value)
        elif _type_str == "NUMERIC":
            self.exact(field, value)
        elif _type_str == "TEXT":
            self.term(field, value)
        elif _type_str == "TAG":
            self.tags(field, value)


    def _single_text(self, field:str) -> str:
        """ """

        current = self.qset[field]
        is_exact = current.get('is_exact', False)
        _term = current.get('value', "")
        if is_exact:
            return f'\"{_term}\"'
        return _term

    def _process_text_filter(self) -> str:
        text_lists = [self._single_text(field) for field in self._text_fields]
        return " ".join(text_lists)

    def _process_boolean(self) -> str:
        """ Do an exact match on all boolean values """
    
    def _process_geo_filter(self) -> str:
        return ""
    
    def _process_tag_filter(self) -> str:
        return ""
    
    def _process_number_filter(self):
        return ""
    
    """ 
        Convert dictionaries into something we can digest
    """

    def convert_geo_dict(self, name:str, _dict:dict):
        """ Convert a geospecific dictionary """
        values = _dict['values']
        if not is_valid_geo(values):
            logger.error(values)
            return
        _long = values.get("long")
        _lat = values.get("lat")
        _distance = values.get("distance")
        _metric = values.get("metric", "km")
        self.near(field=name, _long=_long, _lat=_lat, distance=_distance, metric=_metric)
    
    def convert_boolean_dict(self, name, _dict):
        pass

    def convert_tags_dict(self, name, _dict):
        pass
    
    def convert_text_dict(self, name, _dict):
        """
            1. Check to see if the  dictionary has all of the required components
        """
        pass

    def convert_numeric_dict(self, name, _dict):
        """
            1. Check to see which kind of query we're doing: greater, lesser, between
            2. Check to see if we have dictionary values required to form a query based on each
            3. If it fits, we'd create a query using the above functions 
        """
        pass


    def from_dict(self, name:str, item:dict):
        """ Take a queryable dictionary and find a query for it"""
        # If this isn't a filter, we're not going to add it into a query
        if not item.get("is_filter"):
            logger.debug("We're not creating a search query from this record")
            return
        logger.success("We are creating a query from this set item")
        _type = str(item.get("type")).upper()
        # print(_type)
        if _type == "GEO":
            self.convert_geo_dict(name, item)
        elif _type == "BOOL":
            self.convert_boolean_dict(name, item)
        elif _type == "TEXT":
            self.convert_text_dict(name, item)
        elif _type == "TAGS":
            self.convert_tags_dict(name, item)
        else:
            self.convert_numeric_dict(name, item)


    def build(self):
        """Builds a query to be executed"""
        return ""


class InsertBuilder(object):
    """
        # InsertBuilder

        Builds a redis query given a set of builder parameters
    """
    def __init__(self):
        self._insert_dict = {}
    
    def add_field(self, name, field_type, **values):
        """ Adds a field with the values inside of the kwargs field. Will go through specific formatting."""
        return self

    def build(self):
        return self._insert_dict

    def from_dict(self, name, item:dict):
        """ Get the values from the dictionary for something that's insertable. """
        pass

    def reset(self):
        """ Reset all of the items that are inside of object."""
        pass
