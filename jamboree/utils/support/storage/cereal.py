import lz4.frame
import dill

""" 
    # COMPRESSED SERIALIZATION LIBRARY

    Simply compress and serialize
"""

def serialize(obj):
    """ Should take a complex object and pickle it"""
    pickled = dill.dumps(obj, byref=False)
    compressed = lz4.frame.compress(pickled)
    return compressed

def deserialize(obj):
    """ Should take a serialized object and pickle"""
    decompressed = lz4.frame.decompress(obj)
    unpickled = dill.loads(decompressed)
    return unpickled