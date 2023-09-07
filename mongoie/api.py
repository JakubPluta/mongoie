from typing import Callable

from typing_extensions import Optional

from _writers import to_csv, to_json
from dtypes import MongoQuery, MongoPipeline
from mongoie.mongo import MongoConnector
from utils import ChunkedDataStream


class MongoWriter:
    def __init__(
        self,
        host: str,
        *,
        db: str,
        collection: str,
        query: Optional[MongoQuery, MongoPipeline] = None
    ):
        self.client = MongoConnector(host, db=db)
        self.collection = collection
        self.query = {} if query is None else query
        self.query_mongo: Callable = (
            self.client.aggregate
            if isinstance(query, (list, MongoPipeline))
            else self.client.find
        )

    def to_json(self, file_path: str, **kwargs):
        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        chunks = ChunkedDataStream(cursor)
        to_json(chunks, file_path)

    def to_csv(self, file_path: str, **kwargs):
        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        chunks = ChunkedDataStream(cursor)
        to_csv(chunks, file_path)
