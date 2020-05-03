import os
import re
import json
import time
import warnings
from contextlib import suppress
from copy import copy
from pprint import pprint
from typing import Any, Dict, List, Optional
warnings.simplefilter(action='ignore', category=FutureWarning)
# from cytoolz import unique

"""
NOTE: 

After working with this. I realized how much I fucked up. Will need to fix this. 
Not only did I not know what I was doing, I rushed it and didn't design correctly.

1. Sharpen the old interfaces (solid variables, consistent abstract classes)
2. separate code way more
3. Make take existing use cases (after I've finished this sprint) and make it easier
4. Reduce computation requirements with dicts (objects as often as possible)
5. Make the code super traceable
    - This means keeping track of alters that happen.
    - Maybe have a transition field we write to upon changing tables
    - When you create a schema log the means in which you created that schema. 
    - If you created a schema from a json, take note of that.
6. Make it more decoupled and composable
    - Take their styling, and figure out how to give it some magic. 
    - Use modifiers 
7. Figure out a better way of dealing with schemas (hashes all the way)
8. Move away all of the helper functions to different spots
9. Using classes as parameters to increase certainty in spots
10. Ways of tracking changes with hashes
11. Pipes in many of the places to reduce lookups
12. Have edge cases covered all the way (you'll figure that out through use)
13. Facade pattern
14. Practice use cases using a facade pattern first
    - Access the client directly if you need to.
    - Have controlled ways of messing with pipes
    - Move hella more code from event sourcing to this
    - Come up with automated adds to redisearch if possible. 
        - This will allow you to create dynamic schemas 
15. Come up with better relational patterns (before attempting graphs)
16. Lots of method objects if possible.
17. Do something entirely different with the query builder (use their interface more) and perhaps a builder pattern or something.
18. Reduce the number of loops, especially when not in C.
19. Have some native pagination done.
    - Configuration as a whole should be better. You improved that for managing the backtest, you can do it again for this.
20. Enums, More Constraints and Tests
21. Maybe having some predefined query wrappers would save time fucking with that
22. Do more efficient queries using the info()
23. Prevent fall off conditions for pipes
    - Wrap the copy the current processor when using a wrapped call.
24. Create a SchemaClass of your own
    `
        class RequirementsInteraction:
            # Used to track metadata fields
            description_fields = *fields 
            # Figure out a way to attach a psha to the requirements, even with different variables
            # Find a 
            field = TypeObject(**parameters) # parameters here describe how the data will be used
            field = TypeObject(**parameters)
            field = TypeObject(**parameters)
            field = TypeObject(**parameters)
            field = TypeObject(**parameters)
        
        def alternative(self, schema:SearchSchema):
            # Would search create two keys, first would be the default key, second would be the expanded one
        
        def prepare(self, redisearch_object):
            # if there was any alter command lately, we would
            pass
    `


"""

from addict import Dict as ADict
from cerberus import Validator
from loguru import logger
from redis.exceptions import ResponseError
from redisearch import Client, Query

from jamboree import Jamboree
from jamboree.base.processors.abstracts import Processor
from jamboree.utils.core import consistent_hash
from jamboree.utils.support.search import (BaseSearchHandlerSupport,
                                           InsertBuilder, QueryBuilder,
                                           is_generic, is_geo, is_nested,
                                           is_queryable_dict, name_match,
                                           to_field, to_str)



logger.disable(__name__)
"""

    # NOTE

    Basic CRUD operations for the search handler. 
"""


def split_doc(doc):
    return doc.id, ADict(**doc.__dict__)

def dictify(doc, is_id=True):
    item = ADict(**doc.__dict__)
    item.pop("super_id", None)
    item.pop("payload", None)
    if is_id == False:
        item.pop("id", None)

    return item

def single_doc_check_convert(doc):
    item = doc.__dict__
    item_conv = ADict(**item)
    item_conv_id = item_conv.pop("id", None)
    item_conv.pop("payload", None)
    
    return item_conv, bool(item_conv) 

def doc_convert(doc):
    item = doc.__dict__
    item_conv = ADict(**item)
    # item_conv_id = item_conv.pop("id", None)
    item_conv.pop("payload", None)
    return item_conv

class BaseSearchHandler(BaseSearchHandlerSupport):
    def __init__(self):
        super().__init__()
        self._entity = None
        # This will only be here as an example
        self.is_replacement = False
        self.current_replacement = None
        self.is_set_entity = False
                
        # Subs are all of the subfields we would need to search through
        self.subs:Dict[str, BaseSearchHandler] = {}


        self.current_doc_id = None
        self.current_doc_id_list = set()
        self.current_client = None
        
        self.print_sub = False
        self.use_sub_query = False

        self._super_ids = []
        self._sub_ids = []
        self.finished_alter = False



        self.search_sub = False
        self._processor:Optional[Processor] = None


    def __setitem__(self, key:str, value:Any):
        if key not in self.requirements.keys() and (not self.is_replacement):
            return
        self.is_set_entity = False
        self.current_client = None
        if isinstance(value, dict):
            if len(value) == 0: return
            self.handle_input_dict_key(key, value) 
        else:
            _instance_type = type(value)
            # check that the value is the right type
            if is_generic(_instance_type):
                _str_type = to_str(_instance_type)
                self.query_builder.insert_by_type_str(_str_type, key, value)
                self.insert_builder.insert_by_type_str(_str_type, key, value)


    @property
    def replacement(self):
        if self.current_replacement is None:
            self.current_replacement =  BaseSearchHandler()
            self.current_replacement.is_replacement = True
            self.current_replacement.insert_builder.is_replacement = True
            # self.current_replacement.
        return self.current_replacement

    @property
    def entity(self):
        if self._entity is None:
            raise AttributeError("You haven't set entity yet")
        return self._entity
    
    @entity.setter
    def entity(self, _entity:str):
        self._entity = _entity
    

    @property
    def processor(self):
        if self._processor is None:
            raise AttributeError("The processor hasn't been set yet.")
        return self._processor

    @processor.setter
    def processor(self, _processor:Processor):
        self._processor = _processor
        self.set_sub_processors()

    @property
    def requirements(self):
        return self._requirements_str
    
    @requirements.setter
    def requirements(self, _requirements:dict):
        """If we set it here we'd go through each dict item and create string version of each key"""
        # Document id will allow us to figure out which documents are involved with subkeys
        
        _requirements['entity'] = str
        _requirements['super_id'] = str
        self.process_requirements(_requirements)
        if not self.is_sub_key:
            self.create_sub_handlers()

    @property
    def dreq(self):
        return self._dreq
    
    @dreq.setter
    def dreq(self, _req):
        self._dreq = _req
        self.reset()
        self.requirements = _req
        self.replacement.requirements = _req

    @property
    def allrequirements(self):
        return self._dreq
    
    @allrequirements.setter
    def allrequirements(self, _req):
        self._dreq = _req
        self.reset()
        self.requirements = _req
        self.replacement.requirements = _req

    @property
    def doc_id(self):
        """ We get the current doc_id if it exists"""

        return self.current_doc_id

    @doc_id.setter
    def doc_id(self, _doc_id:str):
        self.current_doc_id = _doc_id


    @property
    def verbatim(self):
        return self.query_builder.build_exact()

    @property
    @logger.catch(ResponseError)
    def client(self):
        """client

        Get the client for the user. If it doesn't exist yet, create a new one with the given stop words. 
        
        For subkeys it adds fields if we've created them recently. 

        .. code-block:: python

            >>> self.client.add_document(_id, payload, **records)


        Returns
        -------
        [type]
            A redis connected client. Gets connection from Jamboree processor.
        """
        # self.processor
        if self.current_client is None:
            # We would insert a connection here. Use the connection from the search processor to operate.
            with suppress(ResponseError):
                self.current_client = Client(self.index, conn=self.processor.rconn)
                # print(self.indexable)
                if len(self.indexable) > 0:
                    self.current_client.create_index(self.indexable, stopwords=["but", "there", "these", "they", "this", "to"])
        
        if self.is_sub_key:
            if not self.finished_alter:
                for i in self.indexable:
                    with suppress(ResponseError):
                        self.current_client.alter_schema_add([i])
                self.finished_alter = True


        return self.current_client

    @property
    def general(self):
        return self.query_builder.general

    @general.setter
    def general(self, term:str):
        """ Push a general term into the query. It can only be done once. Don't put it to a filter key."""
        if not isinstance(term, str):
            logger.error("Term isn't a string")
            return
        self.query_builder.general = term
    
    

    """
        This is when things get weird
    """


    def create_sub_handlers(self):
        """ Creates subhandlers for the given index"""
        for name, subkey in self.subfields.items():
            subhandler = BaseSearchHandler()
            subhandler.is_sub_key = True
            subhandler.index = subkey
            subhandler.insert_builder.is_sub = True
            
            self.replacement.subs[name] = copy(subhandler)
            self.subs[name] = subhandler

    def set_sub_processors(self):
        """ If there are any sub queries, set processors to them """
        if len(self.subfields) > 0:
            self.use_sub_query = True
            for name in self.subfields.keys():
                self.subs[name].processor = self.processor
                with suppress(Exception):
                    self.replacement.subs[name].processor = self.processor

    def set_entity(self):
        if self.is_set_entity is False:
            self['entity'] = {
                "type": "TEXT",
                "is_filter": True,
                "values": {
                    "is_exact": True,
                    "term": self.entity
                }
            }
            self.is_set_entity = True



    def verbatim_docs(self):
        built = self.query_builder.build_exact()
        q = (
                Query(built)
                .no_stopwords()
                .paging(0, 1000000)
            )
        results = self.client.search(q)
        result_docs = results.docs
        return result_docs
    

    def general_docs(self):
        built = self.query_builder.build()
        q = (
                Query(built)
                .paging(0, 1000000)
            )
        results = self.client.search(q)
        result_docs = results.docs
        return result_docs

    
    def verbatim_sub_ids(self):
        super_id_set = set()
        sub_id_set = set()
        
        for key, sub in self.subs.items():
            sub.print_sub = True

            built = sub.query_builder.build()
            # logger.warning(built)
            built = built.strip(' ')
            is_falsy = (not built)
            if is_falsy:
                continue
            # logger.error(built)
            verb_items = sub.general_docs()
            current_super_ids = []
            current_subs = []
            for verb in verb_items:
                try:
                    _verb_id = verb.id
                    _super_id = verb.super_id
                    full_dict = verb.__dict__

                    self.keystore.add(_super_id, key, full_dict)
                    current_subs.append(_verb_id)
                    current_super_ids.append(_super_id)
                except Exception as e:
                    logger.error(str(e))
            
            if len(current_super_ids) > 0:
                if len(super_id_set) == 0:
                    super_id_set.update(current_super_ids)
                else:
                    super_id_set = super_id_set.intersection(current_super_ids)
            
            sub_id_set.update(current_subs)
        
        return list(super_id_set), list(sub_id_set)

    def verbatim_doc_ids(self):
        q = Query(self.verbatim).no_content().paging(0, 1000000)
        results = self.client.search(q)
        ids = [res.id for res in results.docs]
        return ids

    def handle_input_dict_key(self, name:str, item:dict):
        """ Figures out where to put the input dictionary for the query """
        if self.is_sub(name):
            # If this is a subkey we'll run the same operation again
            # Check to see if the subkey is empty and has information that is reducible to "type"
            self.use_sub_query = True
            self.search_sub = True
            reqs = self.loaded_dict_to_requirements(item)
            # logger.debug(reqs)
            self.subs[name].requirements = reqs
            for k, v in item.items():
                self.subs[name][k] = v
        else:
            # If it's not queryable don't try adding anything
            if not is_queryable_dict(item):
                return
            self.insert_builder.from_dict(name, item)
            self.query_builder.from_dict(name, item)


    def normal_find(self, limit_ids=None):
        built = self.query_builder.build()
        q = Query(built).paging(0, 1000000)
        # print(built)
        # print(q.query_string())
        if limit_ids is not None and len(limit_ids) > 0:
            q.limit_ids(*limit_ids)

        results = self.client.search(q)
        result_docs = results.docs
        return result_docs
    
    def normal_find_ids(self, limit_ids=None):
        _query = self.query_builder.build()
        q = Query(_query).no_content().paging(0, 1000000)
        if limit_ids is not None and len(limit_ids) > 0:
            q.limit_ids(*limit_ids)
        results = self.client.search(q)
        result_docs = results.docs
        return [res.id for res in result_docs] 



    def sub_find(self):
        sup_ids, sub_ids = self.verbatim_sub_ids()
        logger.success((sup_ids, sub_ids))
        if len(sub_ids) == 0: return []
        results = self.normal_find(limit_ids=sup_ids)
        results_dicts = []
        for result in results:
            _id, idict = split_doc(result)
            
            idict.pop("payload", None)
            subitems = self.keystore.get(_id)
            idict.update(subitems)
            results_dicts.append(idict)
        return results_dicts

    def normal_insert(self, allow_duplicates=False):
        if allow_duplicates == False:
            verbatim_docs = self.verbatim_docs()

            if len(verbatim_docs) > 0 and allow_duplicates == False:

                # Not adding docs because we're not allowing duplicates
                return verbatim_docs[0].id, False
        insert_variables = self.insert_builder.build()
        _doc_id = self.insert_builder.doc_id
        index_name = self.client.index_name
        fields = [i.redis_args()[0] for i in self.indexable]
        with logger.catch(message=f"{index_name} - {fields}", reraise=True):
            self.client.add_document(_doc_id, payload=_doc_id, **insert_variables)

        return _doc_id, True

    def sub_insert(self, allow_duplicates=False):
        _super_id, _did_insert = self.normal_insert(allow_duplicates=allow_duplicates)
        # logger.info(f'Did insert: {_did_insert}')
        if _did_insert:
            for key, sub in self.subs.items():
                if len(sub.insert_builder._insert_dict) > 0:
                    sub.insert_builder.super_id = _super_id
                    sub.normal_insert(allow_duplicates=True)
        return _super_id

    def find_sub_dictionaries(self, super_id):
        """ Finds a subdictionary by superid inside of the database. """
        # Should use the find within function for every subkey
        mega_dict = ADict()
        for key, sub in self.subs.items():
            key_dict = ADict()
            try:
                res = sub.client.search(f'"{super_id}"')
                if res.total == 0:
                    continue
                dd = [dictify(doc, False) for doc in res.docs]
                key_dict[key] = dd[0]
            except ResponseError:
                pass
            mega_dict.update(key_dict)
        return mega_dict

    
    def find(self):
        """Given the items we've set, find all matching items"""
        
        self.set_entity()
        self.keystore.reset()
        if self.use_sub_query and self.search_sub: 
            return self.sub_find()
        normal = self.normal_find()
        if len(self.subs) == 0:
            if len(normal) > 0:
                return [doc_convert(x) for x in normal]
            return normal
        ndicts = []
        for i in normal:
            # print(i)
            _i = dictify(i)
            mega = self.find_sub_dictionaries(_i.id)
            if len(mega) > 0:
                _i.update(mega.to_dict())
            ndicts.append(_i)
        return ndicts
    
    def pick(self, _id:str):
        """ 
            Given an id find the element with the top level id. We aren't searching lower level_ids. 
            
            After we pull all of the 
        """
        self.set_entity()
        self.keystore.reset()
        doc = self.client.load_document(_id)
        dd = doc.__dict__
        doc = ADict(**dd)
        _id = doc.pop("id", None)
        doc.pop("payload", None)
        doc_z = (len(doc) > 0)
        if len(self.subs) == 0:
            if not doc_z:
                return None
            doc.update({"id": _id})
            return doc
        
        if doc_z:
            sub_dicts = self.find_sub_dictionaries(_id)
            # if len(sub_dicts) > 0:
            doc.update(sub_dicts)
            doc.update({"id": _id})
            return doc
        
        return None


    def update(self):
        """
            # UPDATE

            Given the items or ID we've set, partial update every matching document. 
            If we have the document_ids already, replace those items
        """
        self.set_entity()
        self.keystore.reset()

        replacement_variables = self.replacement.insert_builder.build()
        if self.use_sub_query == False:
            doc_ids = self.verbatim_doc_ids()
            batcher = self.client.batch_indexer(chunk_size=len(doc_ids))
            for doc_id in doc_ids:
                batcher.add_document(doc_id, replace=True, partial=True, **replacement_variables)
            batcher.commit()
        else:
            sup_ids, sub_ids = self.verbatim_sub_ids()
            norm_ids = self.normal_find_ids(limit_ids=sup_ids)
            batcher = self.client.batch_indexer(chunk_size=len(norm_ids))
            for doc_id in norm_ids:
                batcher.add_document(doc_id, replace=True, partial=True, **replacement_variables)
            batcher.commit()


            for sub in self.subs.values():
                subreplacement = sub.insert_builder.build()
                if len(subreplacement) > 0:
                    subbatcher = sub.client.batch_indexer(chunk_size=len(sub_ids))
                    for _id in sub_ids:
                        self.client.add_document(_id, replace=True, partial=True, **subreplacement)
                    subbatcher.commit()
    
    def update_id(self, _id):
        self.set_entity()
        self.keystore.reset()
        doc = self.client.load_document(_id)
        doc_dict, is_exist = single_doc_check_convert(doc)
        
        if not is_exist:
            return
        
        replacement_variables = self.replacement.insert_builder.build()
        self.client.add_document(_id, replace=True, partial=True, **replacement_variables)
        doc = self.client.load_document(_id)
        # print(doc)
        # if len(self.subs) > 0:
        #     subreplacement = sub.insert_builder.build()
        #     print(subreplacement)

    # def insert_many(self, list_of_items):
    #     self.client.ba
    #     pass
    
    

    def insert(self, allow_duplicates=False):
        """
            # INSERT

            Given all of the items we've set, add documents
        """
        self.set_entity()
        self.keystore.reset()
        previous_id = None
        if self.use_sub_query:
            previous_id = self.sub_insert(allow_duplicates=allow_duplicates)
        else:
            previous_id, is_add = self.normal_insert(allow_duplicates=allow_duplicates)
        return previous_id



    def remove(self):
        """Remove all documents that match a query.

        Given a query, remove every document that matches the results of that query. 

        ::
            >>> search['name'] = 'sample_name'
            >>> search['category'] = 'sample_query'
            >>> search.remove() 


        """
        self.set_entity()
        self.keystore.reset()
        if self.use_sub_query and self.search_sub:
            removable = set()
            sup_ids, sub_ids = self.verbatim_sub_ids()
            norm_ids = self.normal_find_ids(limit_ids=sup_ids)
            removable = removable.intersection(sup_ids)
            removable = removable.intersection(norm_ids)


            [self.client.delete_document(_id) for _id in removable]
            for sub in self.subs.values():
                for _id in sub_ids:
                    sub.client.delete_document(_id)
        else:
            norm_ids = self.normal_find_ids()
            # print(norm_ids)
            [self.client.delete_document(_id) for _id in norm_ids]
    

    def reset(self):
        """Reset all local variables"""
        self.reset_builders()
        self.is_set_entity = True
        self.is_replacement = False
        self.current_replacement = None
        self.current_client = None
        self.use_sub_query = False

class ExampleSearchHandler(BaseSearchHandler):
    def __init__(self):
        super().__init__()
        self.entity = "example"
        self.dreq = {
            "name": str,
            "category": str,
            "subcategories": dict,
            "secondsub": dict,
            "live": bool,
            "loc": "GEO"
        }
        

def main():
    logger.enable(__name__)
    processor = Jamboree()
    example_handler = ExampleSearchHandler()
    example_handler.processor = processor
    example_handler['name'] = "Boi Gurl"
    example_handler['category'] = "marketsx"
    example_handler['sample_tags'] = ["four", "five", "six"]
    # TODO: Figure out subcategory update
    example_handler['subcategories'] = {
        "hello": "world",
        "country": "US",
    }
    example_handler['secondsub'] = {
        "my": "jac"
    }
    example_handler.replacement['secondsub'] = {
        "my": "jack"
    }
    example_handler['live'] = False

    # example_handler.remove()
    example_handler.insert(allow_duplicates=True)
    records = example_handler.find()
    print("\n\n")
    logger.warning((records, len(records)))

    records = example_handler.find()
    print("\n\n")
    logger.warning((records, len(records)))

    records = example_handler.find()
    print("\n\n")
    logger.warning((records, len(records)))

    # example_handler.update()
    # records = example_handler.find()
    # logger.success(records)




if __name__ == "__main__":
    main()
