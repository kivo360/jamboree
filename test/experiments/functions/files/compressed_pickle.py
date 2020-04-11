import cloudpickle as clp
import lz4.frame


def serialize(obj):
    """ Should take a complex object and pickle it"""
    pickled = clp.dumps(obj)
    compressed = lz4.frame.compress(pickled)
    return compressed

def deserialize(obj):
    """ Should take a serialized object and pickle"""
    decompressed = lz4.frame.decompress(obj)
    unpickled = clp.loads(decompressed)
    return unpickled

class SampleObject(object):
    def __init__(self) -> None:
        self.one = "IAHSUALKS"
        self.two = "AYVUKASAVS"

def main():
    sample = SampleObject()
    ssample = serialize(sample)
    dsample = deserialize(ssample)
    assert sample.one == dsample.one

if __name__ == "__main__":
    main()