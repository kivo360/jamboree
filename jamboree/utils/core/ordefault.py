def dict_validation(obj:dict) -> bool:
    obj_keys = list(obj.keys())
    for x in ['subcategories', 'entity', 'submetatype', 'name', 'metatype', 'category', "abbreviation"]:
        if x not in obj_keys:
            return False
    return True

def default(obj):
    pass