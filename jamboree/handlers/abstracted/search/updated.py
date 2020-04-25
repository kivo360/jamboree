import uuid
import maya

from typing import Optional
from jamboree import Jamboree
from jamboree.base.processors.abstracts import Processor
from jamboree.handlers.default.search import BaseSearchHandler

from loguru import logger

class ParameterizedSearch(BaseSearchHandler):
    """ 
        An abstract over the base search handler. 
        
        Use to avoid using the normal formatting. 

        Example:

        Normally you'd have to use the following:

        ::
            >>> search['item1'] = 'value'
            >>> search['item2'] = 'value'
            >>> search['item3'] = 'value'
            >>> search['item4'] = 'value'
            >>> search['item5'] = 'value'
            >>> search.insert(allow_duplicates=False)

        Instead you'll use the pattern: 
        
        ::
            >>> id_of_insert = search.Create(
            >>>             allow_duplicates=False, 
            >>>             no_overwrite_reqs=False, 
            >>>             item1='value', item2='value', 
            >>>             item3='value', item4='value', 
            >>>             item5='value')
            # The record's id is set here
            '249fabf229374715ae7e65b7061c0faf'
        

        To define a schema we set the variable `self.allrequirements`.


        ::

    """
    
    def __init__(self):
        """ Initialize the function. Pulls from existing SearchHandler

        Add `must_have` in inherited classes. Use to make certain variable names mandetory.

        Example:

        ::
            >>> def __init__(self):
            >>>    self.must_have = ["category", "name", "abbreviation"]
        


            >>> id_of_insert = search.Create(
            >>>             allow_duplicates=True, 
            >>>             no_overwrite_reqs=True)
            
            Would immediately break because `category`, `name` and `abbreviation`
            
            
        """
        super().__init__()
        self.must_have = [] # Forced fields

    def check_requirements(self, items: dict):
        """ Checks that the fields inside of `must_have` are inside of the dictioary we're going to be adding. """
        for _abs in self.must_have:
            if _abs not in items:
                raise AttributeError(
                    f"{_abs} has to be added. The absolute required variables are the following: {self.must_have}"
                )
        # """ 
        #     Insert a document. All fields defined inside of **kwargs.

        #     Parameters:
        #         allow_duplicates (bool): Determines if we want to allow duplicates of the exact same document inside of the search database.
        #         no_overwrite_must_have (bool): Determines if we're only checking for a small range of fields. Identified inside of `self.must_have`
        #         kwargs (Any): Any field we want to add to the database. It's key and value. The databse must have
        # """

    
    

    def Create(self,
               allow_duplicates=False,
               no_overwrite_must_have=False,
               **kwargs) -> str:
        """Insert a new record into redisearch.

        Args:
            allow_duplicates (bool, optional): Determines if you're going to allow for duplicates inside of the database. Defaults to False.
            no_overwrite_must_have (bool, optional): Determines if we're going to allow for more than 1 record that matches `must_have`. Defaults to False.

        Returns:
            str: The inserted record's id
        """
        self.reset()
        self.check_requirements(kwargs)

        if no_overwrite_must_have and len(self.must_have) > 0:
            
            _all = self.FForced(**kwargs)
            if len(_all) > 0:
                return _all[0].id

        for k, v in kwargs.items():
            self[k] = v

        identity = self.insert(allow_duplicates=allow_duplicates)
        return identity

    def UpdateID(self, identity: str, **kwargs):
        """ Updates a record by ID. Gives a warning if you're using a must have variable."""
        self.reset()
        for k, v in kwargs.items():
            self.replacement[k] = v
        self.update_id(identity)

    def UpdateMany(self, search_dict: dict, force_must_have=False, **replacements):
        """Replaces many records for the user. 

        Args:
            search_dict (dict): Search for what we're replacing.
            force_must_have (bool, optional): Checks that the `search_dict` has all of the `must_have` variables. Defaults to False.

        Raises:
            ValueError: If our search parameters or replacement dictionaries are empty.
        """
        self.reset()
        if not bool(search_dict) or not bool(replacements):
            raise ValueError(
                "You need to have query information AND something to replace it with."
            )
        if force_must_have:
            self.check_requirements(search_dict)

        for k, v in search_dict.items():
            self[k] = v

        for k, v in replacements.items():
            self.replacement[k] = v
        self.update()

    def Find(self, general=None,force_must=False, **fields):
        """Find Searches through the database for our records.

        Run a generalized search through the database.

        Keyword Arguments:
            general {str} -- A general term that will allow us to find terms in a fuzzy search (default: {None})
            force_must {bool} -- Checks that the search fields we have contains everything we declare as important `must_have` (default: {False})

        Raises:
            ValueError: If there's nothing for us to search field. Both fields and general are empty.

        Returns:
            [list] -- A list of descriptions. 
        """
        self.reset()
        if general is not None:
            self.general = general
        if not bool(fields):
            if general is not None:
                return self.find()
            raise ValueError("You have to search using something")
        if force_must:
            self.check_requirements(fields)

        for k, v in fields.items():
            self[k] = v
        return self.find()

    def FindById(self, identity: str):
        self.reset()
        remainder = self.pick(identity)
        return remainder
    
    def FindForced(self, **kwargs):
        self.reset()
        self.check_requirements(kwargs)
        for k in self.must_have:
            self[k] = kwargs.get(k)
        _all = self.find()
        return _all
    
    def FForced(self, **kwargs):
        for k in self.must_have:
            self[k] = kwargs.get(k)
        _all = self.find()
        return _all

    def Remove(self, **kwargs):
        self.reset()
        for k, v in kwargs.items():
            self[k] = v
        self.remove()