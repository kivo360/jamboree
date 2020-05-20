from typing import List
from addict import Dict as ADDICT
from jamboree.middleware.procedures import ProcedureManagement, ModelProcedureAbstract
# from jamboree.middleware.procedures import (
#     CremeProcedure,
#     ModelProcedureAbstract,
#     SklearnProcedure,
#     TFKerasProcedure,
#     TorchProcedure,
# )
from loguru import logger


# class ModelProcedureManagement(ProcedureManagement):
#     def __init__(self):
#         super().__init__()
#         self.required_attributes = ["model_type", "online"]
        
#         # Don't forget to look at the 

#     @property
#     def allowed(self) -> List[str]:
#         return ["sklearn", "torch", "keras", "creme"]

    
#     def access(self, key):
#         self.check_allowed(key)
#         spec = {
#             "sklearn": SklearnProcedure(),
#             "torch": TorchProcedure(),
#             "keras": TFKerasProcedure(),
#             "creme": CremeProcedure(),
#         }.get(key)
#         return spec


# if __name__ == "__main__":
#     modelmgt = ModelProcedureManagement()
#     print(modelmgt.access("sklearn"))
