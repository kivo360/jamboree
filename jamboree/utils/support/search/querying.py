from typing import List
class text(object):
    @staticmethod
    def exact(term):
        return {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "term": term,
                "is_exact": True
            }
        }
    
    @staticmethod
    def fuzzy(term):
        return {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "term": f"%{term}%",
                "is_exact": False
            }
        }
    
    @staticmethod
    def orlist(terms:List[str], is_bundle=False):
        _term = text.orliststr(terms, is_bundle)
        return {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "term": _term,
                "is_exact": False
            }
        }

    @staticmethod
    def orliststr(terms:List[str], is_bundle=False):
        if len(terms) == 0:
            return ""
        _term = "|".join(terms)
        if is_bundle:
            _temp = f"({_term})"
            _term = _temp
        return _term
    

class tags(object):
    
    @staticmethod
    def andfieldstr(field, items:List[str]):
        if len(items) == 0:
            return ""
        
        and_fields_str = [f"{field}:{item} " for item in items]
        return and_fields_str