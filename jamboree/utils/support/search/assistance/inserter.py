import re
from typing import List
from jamboree.utils.support.search import ( is_gen_type, is_generic, is_geo, is_valid_geo,
                                            is_nested, name_match, to_field,
                                            to_str, is_valid_text, is_valid_numeric, is_queryable_dict)



def boolean_process(field, is_true=False):
    """ Return a dicionary that has a TEXT value to represent a boolean """
    bstring = "FALSE"
    if is_true:
        bstring = "TRUE"
    return {
        field: bstring
    }

def list_process(field, item_list:List[str]):
    """ Return a dictionary representing a list of tags"""
    # if isinstance(item_list, list):
    if len(item_list) == 0:
        return {

        }
    saved_list = []

    for i in item_list:
        saved_list.append(f"{i}")
    return {
        field: ",".join(saved_list)
    }

def text_process(field:str, term:str, is_exact=False):
    if is_exact:
        return {
            field: term
        }
    return {
        field:term
    }

def number_process(field, number):
    return {
        field: number
    }



def geo_process_dict(field:str, dictionary:dict):
    """ Converts a dictionary into a dictionary string"""
    vals = dictionary['values']
    if not is_valid_geo(vals):
        return {}
    lon = vals.get("long")
    lat = vals.get("lat")
    return {
        field: f"{lon},{lat}"
    }

def num_process_dict(field:str, dictionary:dict):
    d_vals = dictionary['values']
    if is_valid_numeric(d_vals):
        _operation = d_vals.get("operation")
        _upper = d_vals.get("upper")
        _lower = d_vals.get("lower")
        
        if _operation == "greater":
            return number_process(field, _upper)
        elif _operation == "lesser":
            return number_process(field, _lower)
        elif _operation == "between":
            return number_process(field, _upper)
        elif _operation == "exact":
            _is_exact = (_upper == _lower)
            if _is_exact:
                return number_process(field, _upper)
        return {}

def text_process_dict(field, dictionary:dict):
    """ Create a simple text field from the dictionary"""
    values = dictionary.get("values")
    if is_valid_text(values):
        is_exact = values.get("is_exact", False)
        _term = values.get("term", False)
        filtered_term = re.sub('[^a-zA-Z0-9\n\.|\*|\@|\|\_]', ' ', _term)
        return text_process(field, filtered_term, is_exact=is_exact)
    return {

    }


def create_insertable(example:dict):
    insertable = {}
    for k, v in example.items():
        if isinstance(v, list):
            insertable.update(list_process(k, v))
        elif isinstance(v, str):
            insertable.update(text_process(k, v))
        elif isinstance(v, bool):
            insertable.update(boolean_process(k, v))
        elif isinstance(v, float) or isinstance(v, int):
            insertable.update(number_process(k, v))
        elif isinstance(v, dict):
            if not is_queryable_dict(v):
                continue
            if v['type'] == "NUMERIC":
                insertable.update(num_process_dict(k, v))
            
            if v['type'] == "GEO":
                insertable.update(geo_process_dict(k, v))
            
            if v['type'] == "TEXT":
                insertable.update(text_process_dict(k, v))
    return insertable


def main():
    """ Convert a dictionary into an insertable dictionary"""
    example = {
        "maybe": True,
        "gtags": ["one", "two", "three"],
        "current": {
            "type": "NUMERIC",
            "is_filter": True,
            "values": {
                "lower": 33,
                "upper": 0,
                "operation": "between"
            }
        },
        "loc": {
            "type": "GEO",
            "is_filter": True,
            "values": {
                "long": 33,
                "lat": -10,
                "distance": 1.2,
                "metric": "km"
            }
        },
        "exact_text": {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "term": "hello world"
            }
        },
    }
    

    for _ in range(100):
        insertable = create_insertable(example)
        
        print(insertable)
    pass


if __name__ == "__main__":
    main()