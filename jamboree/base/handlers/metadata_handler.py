""" 
DATA HANDLER
---

The data handler does the following:
1. Accepts DataFrame input with or without time index
2. Converts and saves the data
3. Attaches to Metadata (needs to have a metadata handler to ensure effective usage of information)
4. Connects to a MultiDataHandler
    * The MultiDataHandler
"""

import uuid
import time
import maya
from jamboree.base.handlers.main_handler import DBHandler
from jamboree import Jamboree

class MetaHandler(DBHandler):
    """ 
        # MetadataHandler
        --- 
        The time handler is used to both do simple calculations of time and to maintain the head for what ever calculations we're working on.
    """
    def __init__(self):
        super().__init__()
        self.entity = "metadata"
        self.required = {
            "category": str
        }
        self.subcategories = {}
        self.name = ""

    def _save_subcategory_hierarchy(self):
        """ 
            Each layer of the subcategory will go underneath the category below.
            
            ## An example:
            ```
            subcategory1 = {
                "one": "hello",
                "two": "world",
                "three": "suckers",
                "five": "ate"
            }
            subcategory2 = {
                "one": "the",
                "two": "quick",
                "three": "brown",
                "four": "fox"
            }
            ```
            Should equal
            ---
            ```
            subcategory_tree = {
                "one": {
                    "next_level": ["two"], 
                    "values": ["hello", "the"],
                    "name": List[Dict[str, str]] # This is a list that represents all of the data sources we have.
                }
                "two" {
                    "next_level": ["three"], 
                    "values": ["world", "quick"],
                    "name": List[Dict[str, str]] # This is a list that represents all of the data sources we have.
                },
                "three": {
                    "next_level": ["four", "five"],
                    "values": ["suckers", "brown"],
                    "name": List[Dict[str, str]] # This is a list that represents all of the data sources we have.
                },
                "four": {
                    "next_level": [],
                    "values": ["fox"],
                    "name": List[Dict[str, str]] # This is a list that represents all of the data sources we have.
                },
                "five": {
                    "next_level": [],
                    "values": ["ate"],
                    "name": List[Dict[str, str]] # This is a list that represents all of the data sources we have.
                }
            }
            ```


            Use RedisJSON if you need to use this. 


            If you create some form of adaptation of RedisJSON, you'll have to add it both to the `Jamboree` Object.
            

            NOTE: Looking back, I'm realizing you might want to format it differently. 
                - The point is that we're trying to browse through models on a UI.

            Name should be at the bottom of the hierarchy.

            If you have the subcategory:
            ```
                {
                    "one": "something_else"
                }
            ```
            and and name of "johnny". We add a name and key to the name field.
            ```
                {
                    "name": [
                        "johnny": "__data_source_key__"
                    ]
                }
            ```
            We'd then use that key to find the data represented in the DataHandler.
        """
        pass

    def _query_hierarchy_subcategories(self):
        """ Given a hierarchy (subcategories), find the other subcategories at the end, if anything."""
    
    def _query_item_values(self):
        """ Given a hierarchy, find the values at the end, if anything."""
        pass

    def is_data_exist(self) -> bool:
        """ 
            # Does the data exist
        """
        # Check to see if any data exists in a series of subcategories.
        # self.subcategories
        return False
    
    def reset(self):
        pass
    


if __name__ == "__main__":
    jambo = Jamboree()
    metahandler = MetaHandler()
    metahandler.event = jambo
