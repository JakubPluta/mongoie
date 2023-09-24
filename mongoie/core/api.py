from typing import Callable, Union, Any, Optional, Iterable

from pymongo.collection import Collection
from pymongo.cursor import Cursor

from mongoie.core.readers import get_reader
from mongoie.core.writers import get_exporter, to_mongo, write_chunks
from mongoie.dal.mongo import MongoConnector
from mongoie.dtypes import MongoQuery, MongoPipeline, FilePath, MongoCursor
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


class Exporter:
    """Exports data to a file.

    Examples
    --------
    >>> exporter = Exporter(file_path="my_file.csv")
    >>> exporter.execute(data=[1, 2, 3])

    This will export the data `[1, 2, 3]` to the file `my_file.csv`.
    """

    def __init__(
        self,
        file_path: FilePath,
        file_size: Optional[int] = None,
        normalize: Optional[bool] = None,
        **kwargs: Any,
    ):
        """Initialize Exporter

        Parameters
        ----------
        file_path: FilePath
            The path to the file to export the data to.
        file_size: Optional[int] = None
            The maximum size of each file to export to. If `None`, the data will be exported to a single file.
        normalize: bool = True
            Whether to normalize the data before exporting it.
        **kwargs: Any
            Keyword arguments to pass to the data writer.
        """

        self._file_suffix = get_file_suffix(file_path, dot=False)
        self._file_path = file_path

        self._data_writer = get_exporter(self._file_suffix)
        self._normalize = normalize or Settings.NORMALIZE_EXPORTED_DATA

        if file_size and file_size <= 0:
            logger.warning(
                "file size is smaller or equal to 0 -> setting file_size to None"
            )
            file_size = None
        self._file_size = file_size

        for k, v in kwargs.items():
            setattr(self, k, v)

    def _prep_chunks(self, data: Iterable, **kwargs) -> ChunkedDataStream:
        """Prepares chunks of data to be exported.
        Returns
        -------
        ChunkedDataStream :
            A `ChunkedDataStream` object containing the chunks of data to be exported.
        """
        return (
            ChunkedDataStream(data, self._file_size)
            if self._file_size
            else ChunkedDataStream(data)
        )

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
            f"writing data to {file_path}. using {self._data_writer.__name__} writer"
        )
        write_chunks(
            file_path=self._file_path,
            data=data,
            writer_func=self._data_writer,
            chunk_size=Settings.CHUNK_SIZE,
            multi_files=True if self._file_size else False,
            normalize=self._normalize,
            **kwargs,
        )

    def execute(self, **kwargs):
        """Exports the data to the specified file path.

        Parameters
        ----------
        kwargs:
            Keyword arguments to pass to the `_prep_chunks()` and `_export()` methods.
        """
        self._export(data=self._prep_chunks(**kwargs), file_path=self._file_path)


def export_collection(
    collection: Collection,
    file_path: FilePath,
    normalize: Optional[bool] = None,
    file_size: Optional[int] = None,
    **kwargs,
):
    """Exports a collection to a file.

    Parameters
    ----------
    collection: Collection
        The collection to export.
    file_path: FilePath
        The path to the file to export the data to.
    normalize: Optional[bool] = None
        Whether to normalize the data before exporting it. If `None`, the default value (`True`) will be used.
    file_size: Optional[int] = None
        The maximum size of each file to export to. If `None`, the data will be exported to a single file.
    **kwargs: Any
        Keyword arguments to pass to the exporter.

    Examples
    --------
    >>> collection = client.db.my_collection
    >>> export_collection(collection, file_path="my_collection.csv", normalize=True, file_size=1000)

    This will export the collection `my_collection` to the file `my_collection.csv`,
    split into multiple files of at most 1000 records each, with normalizing the data.
    """
    Exporter(file_path, file_size=file_size, normalize=normalize, **kwargs).execute(
        data=collection.find({}), file_path=file_path, **kwargs
    )


def export_cursor(
    cursor: Cursor,
    file_path: FilePath,
    normalize: Optional[bool] = None,
    file_size: Optional[int] = None,
    **kwargs,
):
    """Exports a cursor to a file.

    Parameters
    ----------
    cursor: Cursor
       The cursor to export.
    file_path: FilePath
       The path to the file to export the data to.
    normalize: Optional[bool] = None
       Whether to normalize the data before exporting it. If `None`, the default value (`True`) will be used.
    file_size: Optional[int] = None
       The maximum size of each file to export to. If `None`, the data will be exported to a single file.
    **kwargs: Any
       Keyword arguments to pass to the exporter.

    Returns
    -------
    None

    Examples
    --------
    >>> cursor = client.db.my_collection.find({})
    >>> export_cursor(cursor, file_path="my_cursor.csv", normalize=True, file_size=1000)
    This will export the cursor `cursor` to the file `my_cursor.csv`, split into multiple files of at most 1000 records
     each, with normalizing the data.

    """
    Exporter(file_path, file_size=file_size, normalize=normalize).execute(
        data=cursor, file_path=file_path, **kwargs
    )


def export_from_mongo(
    host: str,
    *,
    db,
    collection: str,
    query: Optional[Union[MongoPipeline, MongoQuery]] = None,
    file_path: FilePath,
    normalize: Optional[bool] = None,
    file_size: Optional[int] = None,
    **kwargs: Any,
):
    """
    Exports data from MongoDB to a file.

    This function uses the `MongoExporter` class to export data from MongoDB to a file.
    The exporter can export to CSV, JSON, and Parquet files.

    Parameters
    ----------
    host: str
        The host to connect to.
    db: str
        The database to use.
    collection: str
        The collection to export.
    query: Optional[Union[MongoPipeline, MongoQuery]] = None
        A query to filter the collection before exporting it. If `None`, all documents in the collection will be exported.
    file_path: FilePath
        The path to the file to export the data to.
    normalize: Optional[bool] = None
        Whether to normalize the data before exporting it. If `None`, the default value (`True`) will be used.
    file_size: Optional[int] = None
        The maximum size of each file to export to. If `None`, the data will be exported to a single file.
    **kwargs: Any
        Keyword arguments to pass to the exporter.

    Returns
    -------
    None

    Examples
    --------
    >>> export_from_host(
    ...     host="localhost:27017",
    ...     db="my_database",
    ...     collection="my_collection",
    ...     file_path="my_collection.csv",
    ...     normalize=True,
    ...     file_size=1000,
    ... )

    This will export the collection `my_collection` from the database `my_database` on the host `localhost`
    to the file `my_collection.csv`, split into multiple files of at most 1000 records each, with normalizing the data.

    """

    client = MongoConnector(host, db=db)
    query = {} if query is None else query
    mongo_query_func: Callable = (
        client.aggregate if isinstance(query, list) else client.find
    )
    cursor = mongo_query_func(collection, query)
    export_cursor(cursor, file_path, normalize, file_size, **kwargs)


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
                logger.debug(
                    "skipping inserting data to mongo: param skip_if_not_empty is True, "
                    f"and collection {collection.name} is not empty (documents: {docs})"
                )
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
