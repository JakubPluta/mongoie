from typing import Callable, Union, Any, Optional


from mongoie.core.readers import get_reader
from mongoie.core.writers import get_exporter, to_mongo
from mongoie.dal.mongo import MongoConnector
from mongoie.dtypes import MongoQuery, MongoPipeline, FilePath
from mongoie.utils import (
    ChunkedDataStream,
    get_file_suffix,
    build_file_name,
    build_file_path,
    validate_file_path,
)
from mongoie.log import get_logger
from mongoie.settings import Settings

logger = get_logger(__name__)


class MongoExporter:
    """Exports data from MongoDB to a file."""

    def __init__(
        self,
        host: str,
        *,
        db: str,
        collection: str,
        query: Optional[Union[MongoQuery, MongoPipeline]] = None,
        file_path: Optional[FilePath] = None,
        normalize: bool = True,
    ):
        """Initialize MongoExporter

        Parameters
        ----------
        host: str
            The host address of the MongoDB server.
        db: str
            The name of the database to export from.
        collection: str
            The name of the collection to export from.
        query: dict | list
            A MongoDB query or pipeline to filter the data.
        file_path: FilePath
            The path to the output file.
        normalize: bool
            Either normalize data when writing to file or ignore normalization
        """

        self.client = MongoConnector(host, db=db)
        self.collection = collection
        self.query = {} if query is None else query
        self.query_mongo: Callable = (
            self.client.aggregate if isinstance(query, list) else self.client.find
        )

        if file_path is None:
            logger.warning(
                "output file path not provided. Setting default format to json"
            )
            self._file_suffix = "json"
            self.file_path = build_file_path(build_file_name(db, collection))
        else:
            self._file_suffix = get_file_suffix(file_path, dot=False)
            self.file_path = file_path

        self.data_writer = get_exporter(self._file_suffix)
        self.normalize = normalize

    def _prep_chunks(self, **kwargs: Any) -> ChunkedDataStream:
        """Prepares chunks of data to be exported.

        Parameters
        ----------
        kwargs :
            Keyword arguments to pass to the `query_mongo()` method.

        Returns
        -------
        ChunkedDataStream :
            A `ChunkedDataStream` object containing the chunks of data to be exported.
        """

        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        return ChunkedDataStream(cursor)

    def _export(self, data: ChunkedDataStream, file_path: FilePath, **kwargs: Any):
        """Exports data to the specified file path.

        Parameters
        ----------
        data:
            The data to be exported.
        file_path:
            The path to the file to export the data to.
        kwargs:
            Keyword arguments to pass to the data writer.
        """

        logger.info(
            f"writing data to {file_path}. using {self.data_writer.__name__} writer"
        )
        self.data_writer(data, file_path, normalize=self.normalize, **kwargs)

    def execute(self, **kwargs):
        """Exports the data to the specified file path.

        Parameters
        ----------
        kwargs:
            Keyword arguments to pass to the `_prep_chunks()` and `_export()` methods.
        """
        self._export(self._prep_chunks(**kwargs), self.file_path, **kwargs)


class MongoImporter:
    """Import file data to mongo collection"""

    def __init__(
        self,
        host: str,
        *,
        db: str,
        collection: str = None,
        file_path: FilePath,
        denormalized: bool = None,
        denormalization_record_prefix: str = None,
        clear_before: bool = None,
    ):
        """Initialize Mongo Importer

        Parameters
        ----------
        host: str
            The host address of the MongoDB server.
        db: str
            The name of the database to import to.
        collection: str
            The name of the collection to import tp.
        file_path: FilePath
            The path to the output file.
        denormalized: bool
            Flag to import denormalized data to mongo - if data was stored in csv or parquet and it's normalized
            e.g record looks like this   address.street then after denormalized it will be address : {"street" : ...}
        denormalization_record_prefix: str
            record prefix for denormalized data
        clear_before: bool
            Flag to clear collection before import to mongo
        """
        self.client = MongoConnector(host, db=db)

        self.collection = self.client.get_collection(collection)

        self._file_suffix = get_file_suffix(file_path, dot=False)
        self.file_path = validate_file_path(file_path)

        self.data_reader = get_reader(self._file_suffix)

        self._denormalized = denormalized or Settings.DENORMALIZE_IMPORTED_DATA
        self._record_prefix = (
            denormalization_record_prefix or Settings.DENORMALIZATION_RECORD_PREFIX
        )
        self._clear_before = clear_before or Settings.CLEAR_COLLECTION_BEFORE_IMPORT

    @staticmethod
    def _import(data, collection, **kwargs: Any):
        """Imports data to mongo from file.

        Parameters
        ----------
        data:
            The data to import.
        collection:
             MongoDB Collection Object
        kwargs:
            Keyword arguments to pass to the data importer.
        """
        if kwargs and kwargs.get("skip_if_not_empty", False) is True:
            docs = collection.count_documents({})
            if docs > 0:
                logger.debug("skipping inserting data to mongo: param skip_if_not_empty is True, "
                             f"and collection {collection.name} is not empty (documents: {docs})")
                return
        to_mongo(data, collection, **kwargs)

    def execute(self, **kwargs: Any):
        """Imports the data to mongo from the specified file path.
        Parameters
        ----------
        kwargs:
            Keyword arguments to pass to the `_import()` method.
        """
        logger.info(
            f"importing data to mongo from file: {self.file_path}. using {self.data_reader.__name__} to read data and"
            f"to_mongo to write data to Mongo(db={self.client.db.name}, coll={self.collection.name}"
        )
        data_gen = self.data_reader(
            self.file_path,
            denormalized=self._denormalized,
            record_prefix=self._record_prefix,
            **kwargs,
        )
        self._import(data_gen, self.collection, **kwargs)


def list_mongo_databases(host: str, db: str):
    """List all MongoDB databases on the given host.

    Parameters
    ----------
    host : str
        The hostname or IP address of the MongoDB server.
    db : str
        The name of the database to connect to.

    Returns
    -------
    list[str]
        A list of database names.

    """

    client = MongoConnector(host, db=db)
    return client.list_dbs()


def list_mongo_collections(host: str, db, regex=None):
    """List all MongoDB collections on the given host, matching the given criteria.

    Parameters
    ----------
    host : str
        The hostname or IP address of the MongoDB server.
    db : str
        The name of the database to list collections from.
    regex : str, optional
        A regular expression to match collection names against.
        Defaults to None, in which case all collections will be returned.

    Returns
    -------
    list[str]
        A list of collection names.

    """

    client = MongoConnector(host, db=db)
    return client.list_collections(regex=regex)


def export_from_mongo(
    host: str,
    *,
    db,
    collection: str,
    query: Optional[Union[MongoPipeline, MongoQuery]] = None,
    file_path: FilePath,
    normalize: bool = True,
    **kwargs: Any,
):
    """Exports data from MongoDB to a file.

    This function uses the `MongoExporter` class to export data from MongoDB to a file.
    The exporter can export to CSV, JSON, and Parquet files.

    Parameters
    ----------
    host: str
        The hostname or IP address of the MongoDB server.
    db: str
        The name of the database to export data from.
    collection: str
        The name of the collection to export data from.
    query: Optional[Union[MongoPipeline, MongoQuery]]
        A MongoDB pipeline or query to filter the data to be exported.
    file_path: FilePath
        The path to the file to export the data to.
    normalize: True
        Flag to normalize data
    kwargs: Any
        Keyword arguments to pass to the exporter.

    Example
    ----------

    ```python
    from pymongo_export import export_from_mongo

    # Export the "users" collection from the "my_database" database to a file
    export_from_mongo(
        host="localhost:27017,
        db="my_database",
        collection="users",
        file_path="/path/to/export.csv",
    )
    ```
    """
    exporter = MongoExporter(
        host=host, db=db, collection=collection, query=query, file_path=file_path,
        normalize=normalize
    )
    exporter.execute(**kwargs)


def import_to_mongo(
    host: str,
    *,
    db: str,
    collection: str,
    file_path: str,
    denormalized: Optional[bool] = None,
    denormalization_record_prefix: Optional[str] = None,
    clear_before: Optional[bool] = None,
    **kwargs: Any,
):
    """Imports data from a file to MongoDB.

    This function uses the `MongoImporter` class to import data from a file to MongoDB.

    Parameters
    ----------
    host: str
        The hostname or IP address of the MongoDB server.
    db: str
        The name of the database to import data to.
    collection: str
        The name of the collection to import data to.
    file_path: str
        The path to the file to import data from.
    denormalized: Optional[bool]
        Whether the data in the file is denormalized.
    denormalization_record_prefix: Optional[str]
        A prefix in denormalized file.
    clear_before: Optional[bool]
        Whether to clear the collection before importing data.
    kwargs: Any
        Keyword arguments to pass to the importer.

    Example
    ----------

    ```python
    from pymongo_export import import_to_mongo

    # Import the data from the "users.csv" file to the "my_database" database, "users" collection
    import_to_mongo(
        host="localhost:27017",
        db="my_database",
        collection="users",
        file_path="users.csv",
    )
    ```
    """
    importer = MongoImporter(
        host=host,
        db=db,
        collection=collection,
        file_path=file_path,
        denormalized=denormalized,
        denormalization_record_prefix=denormalization_record_prefix,
        clear_before=clear_before,
    )
    importer.execute(**kwargs)


__all__ = [
    "MongoExporter",
    "MongoImporter",
    "import_to_mongo",
    "export_from_mongo",
    "list_mongo_collections",
    "list_mongo_databases",
]
