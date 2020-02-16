from abc import ABC
from redis import Redis
from typing import List, Set
from jamboree.utils.helper import Helpers
import pickle

class ModelHandlerBase(ABC):

    def save_torch(key:str, model, version:int):
        raise NotImplementedError

    def save_keras(key:str, model, version:int):
        raise NotImplementedError

    def load_model(key:str, version:int):
        raise NotImplementedError

    def load_model_by_version(key:str):
        raise NotImplementedError

    def get_latest_version(key:str) -> int:
        raise NotImplementedError

    def get_model_type(key:str) -> str:
        raise NotImplementedError

    def get_number_of_versions(key:str) -> int:
        raise NotImplementedError

class ModelHandler(ModelHandlerBase):
    def __init__(self, redisConnection):
        self.redis = redisConnection
        self.helper = Helpers()

    def dictify(self, model, optimizer, modelType:str)->dict:
        modelDict = {
            'model':model,
            'optimizer':optimizer,
            'type':modelType
        }

        return modelDict

    def dedictify(self, modelDict:dict):
        model = modelDict['model']
        optimizer = modelDict['optimizer']
        return model, optimizer


    def save_torch(self, key:str, model, optimizer, version:int):
        key = self.helper.generate_hash(key)
        modelDict = self.dictify(model, optimizer, "Torch")
        pickled = pickle.dumps(modelDict)
        scored = {pickled:version}
        self.redis.zadd(key, scored)

    def save_keras(self, key:str, model, optimizer, version:int):
        key = self.helper.generate_hash(key)
        modelDict = self.dictify(model, optimizer, "Keras")
        pickled = pickle.dumps(modelDict)
        scored = {pickled:version}
        self.redis.zadd(key, scored)

    def load_latest_model(self, key:str):
        key = self.helper.generate_hash(key)
        pickled = self.redis.zrange(key, -1, -1)
        modelDict = pickle.loads(pickled[0])
        return(self.dedictify(modelDict))

    def load_model_by_version(self, key:str, version:int):
        key = self.helper.generate_hash(key)
        pickled = self.redis.zrange(key, version, version)
        modelDict = pickle.loads(pickled[0])
        return(self.dedictify(modelDict))

    def get_latest_version(self, key:str):
        key = self.helper.generate_hash(key)
        pickled = self.redis.zrange(key, -1, -1, withscores=True)
        return(pickled[0][1])

    def get_model_type(self, key:str):
        key = self.helper.generate_hash(key)
        pickled = self.redis.zrange(key, -1, -1)
        modelDict = pickle.loads(pickled[0])
        return modelDict['type']

    def get_number_of_versions(self, key:str):
        key = self.helper.generate_hash(key)
        return(self.redis.zcount(key,'-inf', '+inf'))

