import base64
import ujson
import orjson
from cytoolz import keyfilter


def consistent_hash(query: dict) -> str:
    _hash = ujson.dumps(query, sort_keys=True)
    _hash = base64.b64encode(str.encode(_hash))
    _hash = _hash.decode('utf-8')
    return _hash

def consistent_unhash(_hash:str) -> str:
    """ Take a consistent hash (sorted) and turn it back into a dictionary"""
    decoded_hash = base64.b64decode(_hash).decode('utf-8')
    _hash_dict = ujson.loads(decoded_hash)
    return _hash_dict


def omit(blacklist, d):
    return keyfilter(lambda k: k not in blacklist, d)


