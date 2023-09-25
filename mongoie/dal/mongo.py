from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

from mongoie.dtypes import (
    MongoCursor,
    MongoDatabase,
    MongoQuery,
    MongoPipeline,
)
from mongoie.log import get_logger

logger = get_logger(__name__, "DEBUG")


class MongoConnector:
    """MongoDB connector - a client-side representation of a MongoDB cluster."""

    def __init__(
        self,
        mongo_uri: Optional[Union[str, Sequence[str]]] = None,
        *,
        db: str,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        mongo_uri: MongoDB host(s).
        db: MongoDB database name.
        kwargs : additional keyword arguments
        """
        self._client = MongoClient(
            host=mongo_uri,
            connect=False,  # Connect on the first operation.
            **kwargs,
        )
        self.chunk_size: int = kwargs.get("chunk_size", 1000)
        self.db = db

    def __repr__(self) -> str:
        """Represent MongoConnector as a string."""
        return (
            f"MongoConnector(host={self._client.HOST}, port={self._client.PORT}, db={self.db}"
            f", chunk_size={self.chunk_size})"
        )

    def __enter__(self) -> MongoConnector:
        """Connect to the resource and return self."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    @property
    def db(self) -> MongoDatabase:
        """Get MongoDB database by database name."""
        return self._db

    @db.setter
    def db(self, database: str):
        """Set MongoDB database."""
        self._db = self.get_db(database)

    @classmethod
    def from_dict(cls, params: Dict[str, Any]):
        """Create MongoConnector from dictionary."""
        return cls(**params)

    def get_db(self, database: Optional[str] = None) -> MongoDatabase:
        """Get mongo database by database name.

        :param database: name of mongodb database
        :return: A Mongo database"""
        return self._client.get_database(database)

    def get_collection(self, collection: str, **kwargs) -> Collection:
        """Get MongoDB collection."""
        return self.db.get_collection(collection, **kwargs)

    def get_collection_count(
        self, collection: str, filter_: Optional[MongoQuery] = None, **kwargs
    ) -> int:
        """Count number of documents in collection. If filter provided then collection will be filtered before count,
        default filter is None.

        Parameters
        ----------
        collection : name of collection
        filter_ : filter eq: {"user_id" : {"$eq" : "abcd123"}}

        Returns
        -------
        int: documents count in given collection"""
        return self.db.get_collection(collection).count_documents(
            filter_ or {}, **kwargs
        )

    def connect(self):
        """Connect to MongoDB."""
        logger.info(
            f"opening connection to mongodb: host: {self._client.HOST} port: {self._client.PORT} db: {self._db}"
        )
        return self

    def disconnect(self):
        """Disconnect from MongoDB."""
        logger.info(
            f"closing connection to mongodb: host: {self._client.HOST} port: {self._client.PORT} db: {self._db}"
        )
        self._client.close()

    def find(
        self, collection: str, query: Optional[MongoQuery] = None, **kwargs
    ) -> MongoCursor:
        """Finds documents in a collection.

        Args:
            collection: The name of the collection.
            query: The query to be used.
            **kwargs: Keyword arguments for the `MongoDB` `find()` method.

        Returns:
            A `MongoDB` cursor.

        """
        return self.db[collection].find(query or {}, {"_id": 0}, **kwargs)

    def aggregate(
        self,
        collection: str,
        pipeline: Optional[MongoPipeline] = None,
        **kwargs,
    ) -> MongoCursor:
        """Aggregates documents in a collection.

        Args:
            collection: The name of the collection.
            pipeline: The aggregation pipeline.
            **kwargs: Keyword arguments for the `MongoDB` `aggregate()` method.

        Returns:
            A `MongoDB` cursor.

        """
        return self.db[collection].aggregate(pipeline or [], **kwargs)

    def list_collections(self, regex: Optional[str] = None) -> List[str]:
        """Get a list of all the collection names in this database."""
        filter_ = {"name": {"$regex": regex}} if regex else None
        return self.db.list_collection_names(filter=filter_)

    def list_dbs(self) -> List[str]:
        """Get a list of the names of all databases on the connected server."""
        return self._client.list_database_names()

    def check_connection(self) -> None:
        """Check if can establish connection to mongodb"""
        try:
            self._client.admin.command("ismaster")
            logger.info(f"{self._client.HOST} {self._client.PORT} connected ")
        except ConnectionFailure:
            logger.error("server is not available")
