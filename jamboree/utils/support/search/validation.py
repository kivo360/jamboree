
from cerberus import Validator
from redisearch import TextField, NumericField, TagField, GeoField
# from jamboree.utils.support.search import filtration_schemas

_global_validator = Validator(require_all=True, allow_unknown=True)
# _filtration_schemas = filtration_schemas()

class Geo(type):
    """ A geolocational type for """
    def __call__(cls):
        return cls.__new__(cls)
    def __repr__(self):
        return "GEO"
    
    def __str__(self):
        return "GEO"
    

def is_nested(d):
    return any(isinstance(i,dict) for i in d.values())

def is_gen_type(item, _type):
    try:
        return isinstance(item, _type) or issubclass(item, _type) or item == _type
    except:
        return False

def name_match(item:str, name:str):
    return item.lower() == name.lower()


def is_generic(_k):
    return _k in [str, float, int, list, bool]


def is_geo(k) -> bool:
    if is_gen_type(k, Geo):
        return True
    
    if is_gen_type(k, str):
        if name_match(k, "geo"):
            return True
    return False

def to_str(i):
    """Converts the item to a string version of it"""
    if i == bool:
        # This will be text that we'll force exact queries on
        return "BOOL"
    elif i == float or i == int:
        return "NUMERIC"
    elif i == str:
        return "TEXT"
    elif i == list:
        return "TAG"


def to_field(k, v):
    if v == "BOOL":
        return TextField(k, sortable=True)
    elif v == "NUMERIC":
        return NumericField(k, sortable=True)
    elif v == "TEXT":
        return TextField(k)
    elif v == "TAG":
        return TagField(k)
    else:
        return GeoField(k)



"""
    Dictionary Validation
"""



def is_valid_geo(_dict:dict):
    """ That we have the appropiate values """
    schema = {
        "long": {"type": "number"},
        "lat": {"type": "number"},
        "distance": {"type": "number", "required":False},
        "metric": {"type": "string", "allowed": ["m","km","mi","ft"], "required":False}
    }
    return _global_validator.validate(_dict, schema)

def is_valid_bool(_dict:dict):
    """ That we have the appropiate values to create a query function for a boolean """
    schema = {
        "toggle": {"type": "boolean"},
    }
    return _global_validator.validate(_dict, schema)

def is_valid_numeric(_dict:dict):
    """ That we have the appropiate values to do a numeric query """
    schema = {
        "operation": {"type": "string", "allowed": ['greater', 'lesser', 'between', 'exact']},
        "upper": {"type": "number"}, 
        "lower": {"type": "number"}
    }
    return _global_validator.validate(_dict, schema)

def is_valid_tags(_dict:dict):
    schema = {
        "operation": {"type": "string", "allowed": ['and', 'or']},
        "tags": {"type": "list", "schema": {"type": "string"}}, 
    }
    return _global_validator.validate(_dict, schema)

def is_valid_text(_dict:dict):
    schema = {
        "term": {"type": "string"},
        "is_exact": {"type": "boolean", "required":False}, 
    }
    return _global_validator.validate(_dict, schema)

def is_queryable_dict(_dict:dict):
    """ """
    schema = {
        "type": {
            "type": "string", 
            "allowed": ["GEO", "TEXT", "BOOL", "NUMERIC", "TAG"]
        },
        "is_filter": {
            "type": "boolean"
        },
        "values": {
            "type": "dict"
        }
    }
    return _global_validator.validate(_dict, schema)

# Specific queryable information


def main():
    _search_item = {
        "type": "GEO",
        "is_filter": False,
        "values": {
            "long": 33,
            "lat": -10,
            "distance": 1,
            "metric": "km"
        }
    }

    _numeric_search_item = {
        "operation": "between",
        "upper": 0,
        "lower": 0
    }

    _bool_search_values = {
        "toggle": True
    }

    print(is_queryable_dict(_search_item))
    print(is_valid_numeric(_numeric_search_item))
    print(is_valid_bool(_bool_search_values))


if __name__ == "__main__":
    main()