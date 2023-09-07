import os
from typing import Any, Dict, Union, List
from pymongo.database import CommandCursor, Database, Collection
from pymongo.cursor import Cursor


FilePath = Union[str, os.PathLike]
MongoDocument = Dict[Any, Any]
MongoAggregationCursor = CommandCursor[MongoDocument]
MongoFindCursor = Cursor[MongoDocument]
MongoCursor = Union[MongoAggregationCursor, MongoFindCursor]
MongoDatabase = Database
MongoCollection = Collection
MongoQuery = Dict[Any, Any]
MongoPipeline = List[Dict[Any, Any]]
