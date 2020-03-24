import uuid
import time
from loguru import logger
import maya
from jamboree.handlers.default import BlobStorageHandler
from jamboree import Jamboree


class ModelHandler(BlobStorageHandler):
    def __init__(self) -> None:
        self.required = {
            "name": str,
            "library": str,
            "info": dict # other information regarding the library type.  
        }


def main():
    pass


if __name__ == "__main__":
    pass