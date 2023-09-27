from __future__ import annotations

from typing import Optional, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

from mongoie.dtypes import (
    MongoCursor,
    MongoQuery,
    MongoPipeline,
)
from mongoie.log import get_logger

logger = get_logger(__name__, "DEBUG")


class MongoConnector(MongoClient):
    """MongoClient wrapper - with some helper methods"""

    def __repr__(self) -> str:
        """Represent MongoConnector as a string."""
        return f"MongoConnector(host={self.HOST}, port={self.PORT}"

    def get_collection(self, database: str, collection: str, **kwargs) -> Collection:
        """Get MongoDB collection.

        Parameters
        ----------
        database: The name of the database
        collection : name of collection

        Returns
        -------
        Collection
            mongo collection object

        """
        db = self.get_database(database)
        return db.get_collection(collection, **kwargs)

    def get_collection_count(
        self,
        database: str,
        collection: str,
        filter_: Optional[MongoQuery] = None,
        **kwargs,
    ) -> int:
        """Count number of documents in collection. If filter provided then collection will be filtered before count,
        default filter is None.

        Parameters
        ----------
        database: The name of the database
        collection : name of collection
        filter_ : filter eq: {"user_id" : {"$eq" : "abcd123"}}

        Returns
        -------
        int: documents count in given collection"""
        return self.get_collection(database, collection).count_documents(
            filter_ or {}, **kwargs
        )

    def find(
        self,
        database: str,
        collection: str,
        query: Optional[MongoQuery] = None,
        **kwargs,
    ) -> MongoCursor:
        """Finds documents in a collection.

        Args:
            database: The name of the database
            collection: The name of the collection.
            query: The query to be used.
            **kwargs: Keyword arguments for the `MongoDB` `find()` method.

        Returns:
            A `MongoDB` cursor.

        """
        return self.get_collection(database, collection).find(
            query or {}, {"_id": 0}, **kwargs
        )

    def aggregate(
        self,
        database: str,
        collection: str,
        pipeline: Optional[MongoPipeline] = None,
        **kwargs,
    ) -> MongoCursor:
        """Aggregates documents in a collection.

        Args:
            database: The name of the database
            collection: The name of the collection.
            pipeline: The aggregation pipeline.
            **kwargs: Keyword arguments for the `MongoDB` `aggregate()` method.

        Returns:
            A `MongoDB` cursor.

        """
        return self.get_collection(database, collection).aggregate(
            pipeline or [], **kwargs
        )

    def list_collections(self, database: str, regex: Optional[str] = None) -> List[str]:
        """Get a list of all the collection names in this database."""
        filter_ = {"name": {"$regex": regex}} if regex else None
        return self.get_database(database).list_collection_names(filter=filter_)

    def check_connection(self) -> None:
        """Check if can establish connection to mongodb"""
        try:
            self.admin.command("ismaster")
            logger.info(f"{self.HOST} {self.Port} connected ")
        except ConnectionFailure:
            logger.error("server is not available")
