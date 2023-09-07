from typing import Callable, Union

from typing_extensions import Optional

from mongoie.core.writers import to_csv, to_json
from mongoie.dal.mongo import MongoConnector
from mongoie.dtypes import MongoQuery, MongoPipeline
from mongoie.utils import ChunkedDataStream


class MongoWriter:
    def __init__(
        self,
        host: str,
        *,
        db: str,
        collection: str,
        query: Optional[Union[MongoQuery, MongoPipeline]] = None
    ):
        self.client = MongoConnector(host, db=db)
        self.collection = collection
        self.query = {} if query is None else query
        self.query_mongo: Callable = (
            self.client.aggregate
            if isinstance(query, list)
            else self.client.find
        )

    def _prep_chunks(self, **kwargs):
        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        return ChunkedDataStream(cursor)

    def to_json(self, file_path: str, **kwargs):
        to_json(self._prep_chunks(**kwargs), file_path)

    def to_csv(self, file_path: str, **kwargs):
        to_csv(self._prep_chunks(**kwargs), file_path)


def write_mongo_to_json(host, *, db, collection, query = None,  file_path: str, **kwargs):
    writer = MongoWriter(host=host, db=db, collection=collection, query=query)
    writer.to_json(file_path, **kwargs)


def write_mongo_to_csv(host, *, db, collection, query = None, file_path: str, **kwargs):
    writer = MongoWriter(host=host, db=db, collection=collection, query=query)
    writer.to_csv(file_path, **kwargs)