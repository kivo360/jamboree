import re
import uuid
from typing import Dict
from loguru import logger
from jamboree.utils.support.search import (
    is_valid_geo, is_valid_bool, is_valid_numeric, is_valid_tags, is_valid_text
)
from jamboree.utils.support.search.assistance import inserter

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
        self._general = ""
        self.all_exact = False
        self.super_id = None

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
        
    @property
    def general(self):
        return self._general
    
    @general.setter
    def general(self, _general):
        self._general = _general

    def exact(self, field, num):
        placeholder = {
            "filter": "number",
            # "operation": "exact",
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
            # "operation": "greater",
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
            # "operation": "less",
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
            # "operation": "between",
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
    
    def tags(self, field, tags:list, op="OR"):
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
        updated_term = re.sub('[^a-zA-Z0-9\n\.|\*|\@|\|\_]', ' ', _term)
        placeholder = {
            "filter": "text",
            "is_exact": is_exact,
            "value":  updated_term
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
        
        if is_exact or self.all_exact:
            return f'@{field}:\"{_term}\"'
        return f"@{field}:{_term}"

    def _process_text_filter(self) -> str:
        text_lists = [self._single_text(field) for field in self._text_fields]
        joined = " ".join(text_lists)
        joined = joined.strip()
        return joined


    def _single_bool(self, field:str) -> str:
        """ """

        current = self.qset[field]
        _term = current.get('value', "")
        return f"@{field}:\"{_term}\""

    def _process_boolean(self) -> str:
        """ Do an exact match on all boolean values """
        bool_list = [self._single_bool(field) for field in self._boolean_fields]
        joined = " ".join(bool_list)
        return joined
    
    def _single_geo(self, field:str):
        """ Turn a geospacial query into a string """
        current = self.qset[field].get("value")

        lon = current.get("long")
        lat = current.get("lat")
        dist = current.get("distance")
        metric = current.get("metric")
        _geo = f"@{field}:[{lon} {lat} {dist} {metric}]"
        return _geo

    def _process_geo_filter(self) -> str:
        geo_list = [self._single_geo(field)  for field in self._geo_fields]
        joined = " ".join(geo_list)
        return joined
    
    def _single_tag(self, field:str):
        current = self.qset[field]
        values = current.get("value")
        _tags = values.get("tags")
        if len(_tags) == 0:
            return ""
        join_val = "|"
        _joined = join_val.join(_tags)
        _joined = _joined.strip()
        _tag_filter = f"@{field}: {{ {_joined} }}"
        return _tag_filter

    def _process_tag_filter(self) -> str:
        tag_list = [self._single_tag(field) for field in self._tag_fields]
        joined = " ".join(tag_list)
        trimmed = joined.strip()
        return trimmed
    

    def _single_num(self, field):
        current = self.qset[field]

        _value = current.get("value")
        up = _value.get("upper")
        down = _value.get("lower")
        if up == down:
            return f"@{field}:[{up} {up}]"
        
        return f"@{field}: [{down} {up}]"
        


    def _process_number_filter(self):
        tag_list = [self._single_num(field) for field in self._number_fields]
        joined = " ".join(tag_list)
        return joined
    
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
    
    def convert_text_dict(self, name:str, _dict:dict):
        """
            1. Check to see if the  dictionary has all of the required components
        """
        values = _dict['values']
        if not is_valid_text(values):
            logger.error(values)
            return
        _term = values.get("term")
        _is_exact = values.get("is_exact", False)
        self.term(field=name, _term=_term, is_exact=_is_exact)

    def convert_numeric_dict(self, name:str, _dict:dict):
        """
            1. Check to see which kind of query we're doing: greater, lesser, between
            2. Check to see if we have dictionary values required to form a query based on each
            3. If it fits, we'd create a query using the above functions 
        """
        values = _dict['values']
        if not is_valid_numeric(values):
            logger.error(values)
            return
        _operation = values.get("operation")
        _up = values.get("upper", 0)
        _low = values.get("lower", 0)

        if _operation == "greater":
            self.greater(name, _low)
        elif _operation == "lesser":
            self.less(name, _up)
        elif _operation == "between":
            self.between(name, _up, _low)
        elif _operation == "exact":
            self.exact(name, _low)


    def from_dict(self, name:str, item:dict):
        """ Take a queryable dictionary and find a query for it"""
        # If this isn't a filter, we're not going to add it into a query
        if not item.get("is_filter"):
            logger.debug("We're not creating a search query from this record")
            return
        
        _type = str(item.get("type")).upper()
        
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
        processed_text = self._process_text_filter()
        processed_bool = self._process_boolean()
        processed_num = self._process_number_filter()
        processed_tags = self._process_tag_filter()
        processed_geo = self._process_geo_filter()
        joined_query_string = " ".join([processed_text, processed_tags, processed_num, processed_bool, processed_geo])
        final_joined = self.general + joined_query_string
        return final_joined

    def build_exact(self):
        self.all_exact = True
        processed = self.build()
        self.all_exact = False
        return processed.strip()

class InsertBuilder(object):
    """
        # InsertBuilder

        Builds a redis query given a set of builder parameters
    """
    def __init__(self):
        self._insert_dict = {}
        self.doc_id = ""
        self.is_replacement = False
        self.is_sub = False
        self.super_id = None


    def add_field(self, name, field_type, **values):
        """ Adds a field with the values inside of the kwargs field. Will go through specific formatting."""
        return self


    
    def boolean(self, field:str, is_true=False):
        new_field = inserter.boolean_process(field, is_true=is_true)
        self._insert_dict.update(new_field)
        return self
    
    def tags(self, field, tags:list, op="OR"):
        new_field = inserter.list_process(field, tags)
        self._insert_dict.update(new_field)
        return self
    
    def within(self, field, _within:list):
        """ 
            # WITHIN
            Essentially a `WHERE x IN` command for SQL

            WHERE x IN ('foo', 'bar','hello world')
        """
        pass

    def term(self, field, _term:str, is_exact=False):
        new_field = inserter.text_process(field, _term, is_exact)
        self._insert_dict.update(new_field)
        return self

    

    def exact(self, field:str, num):
        """ Set's an exact number """
        self._insert_dict[field] = num
        return self
  


    def convert_geo_dict(self, name:str, item:dict):
        new_item = inserter.geo_process_dict(name, item)
        self._insert_dict.update(new_item)

    def convert_text_dict(self, name:str, item:dict):
        new_item = inserter.text_process_dict(name, item)
        self._insert_dict.update(new_item)
    

    def convert_numeric_dict(self, name:str, item:dict):
        new_item = inserter.num_process_dict(name, item)
        self._insert_dict.update(new_item)


    def from_dict(self, name:str, item:dict):
        """ Get the values from the dictionary for something that's insertable. """
        if not item.get("is_filter"):
            logger.debug("We're not creating a search query from this record")
            return
        
        # logger.debug(item)
        _type = str(item.get("type")).upper()
        
        if _type == "GEO":
            self.convert_geo_dict(name, item)
        elif _type == "TEXT":
            self.convert_text_dict(name, item)
        else:
            self.convert_numeric_dict(name, item)

    def insert_by_type_str(self, _type_str:str, field:str, value):
        if _type_str == "BOOL":
            self.boolean(field, is_true=value)
        elif _type_str == "NUMERIC":
            self.exact(field, value)
        elif _type_str == "TEXT":
            self.term(field, value)
        elif _type_str == "TAG":
            self.tags(field, value)



    def build(self):
        """ After all of the dictionaries are set we create a doc_id (for the given index) and we create one"""
        # if 
        self.doc_id = uuid.uuid4().hex
        if self.super_id is not None:
            self._insert_dict['super_id'] = self.super_id
        return self._insert_dict

    def reset(self):
        """ Reset all of the items that are inside of object."""
        self._insert_dict = {}
        self.super_id = None
