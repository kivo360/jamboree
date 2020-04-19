# import random
# import time
# import uuid
# from pprint import pprint
# from typing import Any, List, Optional

# import maya
# from addict import Dict
# from loguru import logger

# from jamboree import Jamboree
# from jamboree.handlers.complex.engines import FileEngine
# from jamboree.middleware.procedures import (ModelProcedureAbstract,
#                                             ModelProcedureManagement,
#                                             ProcedureAbstract,
#                                             ProcedureManagement)
# from jamboree.utils.support.search import querying

# logger.disable(__name__)

# class ModelEngine(FileEngine):
#     """ """

#     def __init__(self, processor, **kwargs):
#         super().__init__(processor=processor, **kwargs)
#         self.pm = ModelProcedureManagement()
#         self.current_procedure = None

#     def init_specialized(self, **kwargs):
#         super().init_specialized(**kwargs)
#         self.model_type = kwargs.get("model_type", "sklearn")

#     def open_context(self):
#         if not self.file_reset:
#             self.reset()
        

#     def close_context(self):
#         current_model = self.model
#         if current_model.changed:
#             extracted = current_model.extract()
#             self.save_file(extracted)
        
#         # probably do some sort of metrics stuff :) 


#     def enterable(self):
#         """ Return the object we want to enter into """
#         return self.model

#     def custom_post_load(self, item):
#         proc = self.procedure
#         proc.dictionary = item
#         proc.verify()
#         self.current_procedure = proc
    

#     @property
#     def procedure(self) -> 'ModelProcedureAbstract':
#         if not self.current_procedure:
#             self.current_procedure = self.pm.access(self.model_type)
#             logger.success(f"Successfully accessed a procedure: {self.current_procedure}")
#         return self.current_procedure
    
#     @procedure.setter
#     def procedure(self, _procedure:'ModelProcedureAbstract'):
#         self.current_procedure = _procedure

#     @property
#     def model(self):
#         if self.current_procedure:
#             self.procedure.verify()
#             return self.procedure
#         raise AttributeError("You haven't added a procedure yet")
    
#     def file_from_dict(self, item:Dict):
#         reloaded = ModelEngine(
#             processor=self.processor,
#             name=item.name,
#             category=item.category,
#             subcategories=item.subcategories,
#             submetatype=item.submetatype,
#             abbreviation=item.abbreviation,
#             model_type=item.submetatype
#         )
#         return reloaded


#     def reset(self):
#         super().reset()


# def file_engine_main():
#     """ 
#         Creating a generic usage of the file engine instead of only model storage. 

#         To test, we're going to entirely duplicate the prior test. 
#         Only we're going to use generic functions and variables. In essensce, rebuild the `ModelEngine` starting with the file handler

#     """

#     from jamboree.middleware.procedures.models import CustomSklearnGaussianProcedure
#     file_name = uuid.uuid4().hex
#     logger.info("Starting file engine experiment")
#     logger.info(f"The file name is: {file_name}")
#     jamboree_processor = Jamboree()
#     with logger.catch(message="There should be no reason for this to fail"):
#         # Initialize a file engine
#         model_engine = ModelEngine(
#             processor=jamboree_processor,
#             name=file_name,
#             category="machine",
#             subcategories={"ml_type": "gaussian"},
#             abbreviation="GAUSS",
#             submetatype="sklearn",
#             blobfile=CustomSklearnGaussianProcedure(),
#         )
#         model_engine.reset()

#         reloaded = model_engine.first(name=file_name)
#         while True:
#             with reloaded as model:
#                 logger.debug(model)


# if __name__ == "__main__":
#     logger.enable(__name__)
#     file_engine_main()
