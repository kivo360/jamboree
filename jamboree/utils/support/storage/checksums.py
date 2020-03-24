import hashlib

def create_checksum(_blob):
    return hashlib.md5(_blob).hexdigest()