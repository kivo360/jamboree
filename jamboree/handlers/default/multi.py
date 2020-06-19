import json
import pprint
import time
import uuid
from copy import deepcopy
from typing import Any, Dict, List

import crayons as cy
import maya
import pandas_datareader.data as web
import ujson
from loguru import logger

from jamboree import JamboreeNew
from jamboree.handlers.abstracted.search import MetadataSearchHandler
from jamboree.handlers.complex.meta import MetaHandler
from jamboree.handlers.default import DataHandler, DBHandler, TimeHandler, Access
from jamboree.middleware.processors import DataProcessorsAbstract, DynamicResample
from jamboree.utils.context import example_space
from jamboree.utils.core import consistent_hash, consistent_unhash
from jamboree.utils.support.search import querying



"""
    NOTE: Will probably inherit this to fix it in private.
    This is dog shit!!!!
    
    A lot of this will be replaced using a task graphs (to manage relationships), redisearch aggregations and pipelines (crafting a tree of interactions)

"""


class MultiDataManagement(Access):
    """ 
        # Multi-Data Handler
        ---

        Here we define how different data sources will be pulled at the same time. 
        
        Given a set of parameters we can select which data sources we want to pull for a given problem. 


        ## Why is this useful?
        ---
        This is useful because we're able to pull together any number of data sources we have stored inside of our database.

        Some usecases of this would be:

        - Getting multiple assets to define a portfolio allocation.
        - Getting different asset classes define pairs to trade on.
        - Getting a superset of different asset classes to predict movements between them.
        - Getting a predefied pair to determine which signals should be used.
    """

    def __init__(self, **kwargs):
        super().__init__()

        self.entity = "multi_data_management"
        """ 
            # Required variables
            ---

            - Set name This is the name of the data sets we're going to have
        """
        self.required = {
            "name": str,
            "category": str,
            "subcategories": dict,
            "metatype": str,
            "submetatype": str,
            "abbreviation": str,
        }
        self.is_robust = False
        self.initialize(**kwargs)
        self.data_handler_list: List[
            DataHandler
        ] = []  # Store the dataset objects we can access at once without redeclaring
        self.dup_check_list = []  # use to check for duplicates in dataset
        
        
        self._meta = MetaHandler()
        self._metasearch = MetadataSearchHandler()
        self.metaid: str = ""
        self.description = kwargs.get("description", None)

    def initialize(self, **kwargs):
        """ A more concentrated place to initialize everything"""
        self.init_handlers()
        self.init_default(**kwargs)
        self.init_required(**kwargs)

    def init_default(self, **kwargs):
        self._episode: str = kwargs.get("episode", uuid.uuid4().hex)
        self._is_live: bool = kwargs.get("live", False)
        self._preprocessor = kwargs.get("preprocessor", DynamicResample("data"))

        self.is_real_filter: bool = kwargs.get("real_filter", True)
        self.metatype = self.entity
        self.category = "universe"
        self.submetatype = "price_bag"

        # self["metatype"] = self.entity
        # self["category"] = "universe"
        # self["submetatype"] = "price_bag"
        
        self.is_event = False

    def init_required(self, **kwargs):
        if len(kwargs) == 0:
            return
        for k, v in self.required.items():
            if k in kwargs:
                value = kwargs.get(k)
                if isinstance(value, v):
                    self[k] = value

    def init_handlers(self):
        self.datasethandler: DataHandler = DataHandler()
        self._time: TimeHandler = TimeHandler()

    @property
    def episode(self) -> str:
        return self._episode

    @episode.setter
    def episode(self, _episode: str):
        self._episode = _episode

    @property
    def live(self) -> bool:
        return self._is_live

    @live.setter
    def live(self, _live: bool):
        self._is_live = _live

    @property
    def sources(self) -> list:
        source_dict = self.latest_dataset_list()
        source_list = source_dict.get("sources", [])
        return source_list

    @property
    def datasets(self) -> List[DataHandler]:
        return self.data_handler_list

    @property
    def time(self) -> "TimeHandler":
        self._time.event = self.event
        self._time.processor = self.processor
        self._time["episode"] = self.episode
        self._time["live"] = self.live
        return self._time

    @property
    def metadata(self):
        self._meta.processor = self.processor
        self._meta["name"] = self.name
        self._meta["category"] = self.category
        self._meta["subcategories"] = self.subcategories
        self._meta["metatype"] = self.entity
        self._meta["submetatype"] = self.submetatype
        self._meta["abbreviation"] = self.abbreviation
        self._meta.description = self.description
        return self._meta

    @property
    def search(self):
        self._metasearch.reset()
        self._metasearch["category"] = querying.text.exact(self.category)
        self._metasearch["metatype"] = querying.text.exact(self.entity)
        self._metasearch["submetatype"] = querying.text.exact(self.submetatype)
        self._metasearch.processor = self.processor
        return self._metasearch

    @property
    def preprocessor(self) -> DataProcessorsAbstract:
        return self._preprocessor

    @preprocessor.setter
    def preprocessor(self, _preprocessor: DataProcessorsAbstract):
        self._preprocessor = _preprocessor

    @property
    def is_next(self) -> bool:
        """ Determine if anything is next in the head"""
        processor = self.processor
        is_live_list: List[bool] = []
        for ds in self.datasets:
            ds.processor = processor
            ds.episode   = self.episode
            ds.live      = self.live
            ds.is_robust = self.is_robust
            is_live_list.append(ds.is_next)

        # Use all to determine if the values are falsey or not
        return all(is_live_list)


    @property
    def source_ids(self) -> List[str]:
        """Get SourceIds

        Get the metadata id for the individual data sources inside of the multi-data set.

        Returns
        -------
        List[str]
            List of hex ids
        """


        _source_ids = []
        for source in self.sources:
            sourceid = self._get_source_id(source)
            if sourceid is not None:
                _source_ids.append(sourceid)
        return _source_ids

    def allvalid(self, item: dict):

        name = item.get("name", None)
        category = item.get("category", None)
        subcategories = item.get("subcategories", None)
        submetatype = item.get("submetatype", None)
        abbreviation = item.get("abbreviation", None)
        if None in [name, category, subcategories, submetatype, abbreviation]:
            return False
        return True

    def add_multiple_data_sources(
        self, sources: List[Dict[str, Any]], alt={}, allow_bypass=False
    ):
        """ Add a dataset list"""
        if allow_bypass == True:
            self.save_dataset_list(sources)
            return
        is_valid = self._validate_added_sources(sources)
        if is_valid:
            self.save_dataset_list(sources)

    def remove_multiple_datasources(
        self, rsources: List[Dict[str, Any]], alt={}, allow_bypass=False
    ):

        existing = self.sources
        if len(existing) == 0 or len(rsources) == 0:
            return

        rhashes = set(consistent_hash(i) for i in rsources)
        ohashes = set(consistent_hash(i) for i in existing)

        remaining_hash = ohashes - rhashes
        remaining = [consistent_unhash(x) for x in remaining_hash]
        sources_dict = {"sources": remaining}
        self.save(sources_dict)

    def add_data_source(self, source: Dict[str, Any]):
        if not isinstance(source, dict):
            logger.error("Not a dictionary. Skipping ... ")
            return
        source_list = [source]
        self.add_multiple_data_sources(source_list)

    def remove_data_source(self, source: Dict[str, Any]):
        if not isinstance(source, dict):
            logger.error("Not a dictionary. Skipping ... ")
            return
        source_list = [source]
        self.remove_multiple_datasources(source_list)

    def add_dataset_handler(self):
        _copy = self.datasethandler.copy()

        str_copy = str(_copy)
        if str_copy in self.dup_check_list:
            return
        self.dup_check_list.append(str_copy)
        self.datasethandler.event = self.event
        self.datasethandler.processor = self.processor
        self.data_handler_list.append(_copy)

    def _is_data_exist(self, source: dict) -> bool:
        """ Check to see if the data exist when putting list of datahandlers together"""
        self.datasethandler.event = self.event
        self.datasethandler.processor = self.processor
        self.datasethandler["name"] = source["name"]
        self.datasethandler["category"] = source["category"]
        self.datasethandler["subcategories"] = source["subcategories"]
        self.datasethandler["submetatype"] = source["submetatype"]
        self.datasethandler["abbreviation"] = source["abbreviation"]
        count = self.datasethandler.count()
        not_zero = count != 0
        if not_zero:
            self.add_dataset_handler()
        return not_zero
    

    def _get_source_id(self, source: dict) -> bool:
        """ Check to see if the data exist when putting list of datahandlers together"""
        self.datasethandler.event               = self.event
        self.datasethandler.processor           = self.processor
        self.datasethandler["name"]             = source["name"]
        self.datasethandler["category"]         = source["category"]
        self.datasethandler["subcategories"]    = source["subcategories"]
        self.datasethandler["submetatype"]      = source["submetatype"]
        self.datasethandler["abbreviation"]     = source["abbreviation"]
        count = self.datasethandler.count()
        not_zero = count != 0

        if not_zero:
            return self.datasethandler.reset()
        return None

    def _add_wo_duplicates(self, original_list: list, new_list: list):
        original_set = set(ujson.dumps(i, sort_keys=True) for i in original_list)
        for item in new_list:
            frozen = ujson.dumps(item, sort_keys=True)
            original_set.add(frozen)
        return [ujson.loads(x) for x in original_set]

    def _remove_invalid_dataset_formats(self, original_list: List[Dict[str, Any]]):
        valid_list = []
        for original in original_list:
            if not self.allvalid(original):
                continue
            valid_list.append(original)
        return valid_list

    def _filter_non_existing_datasets(self, _datasets):
        all_sets = []
        for source in _datasets:
            exist = self._is_data_exist(source)
            if exist == True:
                all_sets.append(source)
        return all_sets

    def _validate_added_sources(self, sources: List[Dict[str, Any]]):
        if not isinstance(sources, list):
            logger.error("Sources is not a list. Skipping ...")
            return False

        if len(sources) == 0:
            logger.error("Sources is empty. Skipping ...")
            return False

        for source in sources:
            if not isinstance(source, dict):
                logger.error(
                    "An item inside of the sources list is not a dictionary. Skipping ..."
                )
                return False

            keys = source.keys()
            if len(keys) == 0:
                logger.error("One of the dictionaries doesn't have a key. Skipping ...")
                return False

            for key in keys:
                if not isinstance(key, str):
                    logger.error(
                        "Dude, stop messing up. One of the keys isn't a string. Skipping ..."
                    )
                    return False
        return True

    # -------------------------------------------------------------------------------------------
    # -------------------------------------------------------------------------------------------
    # --------------------------------- I/O Commands --------------------------------------------
    # -------------------------------------------------------------------------------------------
    # -------------------------------------------------------------------------------------------
    def save_dataset_list(self, sources: List[Dict[str, Any]]):
        self.check()
        # We store the sources list into a dictionary to make it easier for Jamboree to handle
        latest_sources = self.latest_dataset_list()
        latest_source_list = latest_sources.get("sources", [])
        if len(sources) == 0 and len(latest_source_list) == 0:
            sources_dict = {"sources": []}
            self.save(sources_dict)
        else:
            validated_sources = self._remove_invalid_dataset_formats(sources)
            if self.is_real_filter == True:
                validated_sources = self._filter_non_existing_datasets(
                    validated_sources
                )
            _sources = self._add_wo_duplicates(latest_source_list, validated_sources)
            sources_dict = {"sources": _sources}
            self.save(sources_dict)

    def count_dataset_list(self) -> int:
        """ Get the number of data source records we've gathered so far. """
        self.check()
        count = self.count()
        return count

    def latest_dataset_list(self) -> dict:
        self.check()
        latest_list = self.last()
        return latest_list


    

    def _load_dataset_list(self):
        self.check()
        if self.count_dataset_list() > 0:
            latest = self.latest_dataset_list()
            sources = latest.get("sources", [])
            if len(sources) == 0:
                return
            # print(sources)
            for source in sources:
                self._is_data_exist(source)

    def _reset_dataset_list(self) -> None:
        """ Initialize with a nil set of data if nothing exist yet"""
        if self.count_dataset_list() == 0:
            self.add_multiple_data_sources([], allow_bypass=True)

    # def dump(self)

    def reset(self):
        """ Reset the data we're querying for. """
        self._load_dataset_list()
        self._reset_dataset_list()
        self.time.reset()
        self.sync()
        self.metaid = self.metadata.reset()
        return self.metaid

    def step(self, call_type: str = "dataframe"):
        avail_types = ["dataframe", "current"]
        if call_type not in avail_types:
            call_type = "dataframe"

        data_set = {}
        # We can multi-thread this
        for dataset in self.datasets:
            dataset_name = str(dataset)
            dataset.event = self.event
            dataset.processor = self.processor
            dataset.preprocessor = self.preprocessor
            if call_type == "dataframe":
                data_set[dataset_name] = dataset.dataframe_from_head()
            else:
                data_set[dataset_name] = dataset.closest_head()
        """ Remove this time step """
        self.sync()
        return data_set
    

    def sync(self):
        """ Gets all of the datahandlers and synchronize their time object """
        if len(self.datasets) > 0:
            self.time.processor = self.processor
            for data in self.datasets:
                data.live = self.live
                data.episode = self.episode
                data.time = self.time
    

    def pick(self, _id:str):
        # Will probably make this kind of interface common. Get all information by meta_id
        """Pick

        Get multi-dataset by id

        Parameters
        ----------
        _id : str
            MultiData identifier
        """
        item = self.search.FindById(_id)
        if item is None:
            return None
        _multi                  = MultiDataManagement()
        _multi.processor        = self.processor
        _multi['name']          = item['name']
        _multi['abbreviation']  = item['abbreviation']
        _multi['submetatype']   = item['submetatype']
        _multi['category']      = item['category']
        _multi['metatype']      = item['metatype']
        _multi['subcategories'] = (item['subcategories']).to_dict()
        _multi.reset()
        return _multi

    

if __name__ == "__main__":
    with example_space("Multi-Data-Management") as example:
        # set_name = uuid.uuid4().hex
        set_name = "ac688d95336e41bdbe61c5c804d07f1a"
        # jam = Jamboree()
        jam_proc = JamboreeNew()
        multi_data = MultiDataManagement()
        multi_data["set_name"] = set_name
        # multi_data.event = jam
        multi_data.processor = jam_proc
        multi_data.episode = uuid.uuid4().hex
        multi_data.reset()
        dset1 = {
            "name": "shaw",
            "subcategories": {"beautiful": "mind", "it": "is"},
            "category": "pricing",
        }

        dset2 = {
            "name": "shank",
            "subcategories": {"wonderful": "hello", "king": "world"},
            "category": "pricing",
        }

        dset3 = {
            "name": "MSFT",
            "subcategories": {
                "market": "stock",
                "country": "US",
                "sector": "techologyyyyyyyy",
            },
            "category": "markets",
        }

        dset4 = {
            "name": "AAPL",
            "subcategories": {
                "market": "stock",
                "country": "US",
                "sector": "techologyyyyyyyy",
            },
            "category": "markets",
        }

        full_set = [dset1, dset2, dset3, dset4]
        pprint.pprint(multi_data.sources)
        multi_data.add_multiple_data_sources(full_set)
        # Check to make sure we aren't adding any dummy sources
        multi_data.time.head = maya.now().subtract(weeks=200, hours=14)._epoch
        multi_data.time.change_stepsize(microseconds=0, days=1, hours=0)
        multi_data.time.change_lookback(microseconds=0, weeks=4, hours=0)
        multi_data.sync()
        for _ in range(1000):
            multi_data.step()
            multi_data.time.step()
            current_time = multi_data.time.head
            print((f"Step {current_time}"))
