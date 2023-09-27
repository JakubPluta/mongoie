import os
from typing import Callable, Union, Any, Optional, Iterable

from pymongo.collection import Collection
from pymongo.cursor import Cursor

from mongoie.core.readers import get_reader
from mongoie.core.writers import get_exporter, to_mongo, write_chunks
from mongoie.dal import MongoConnector
from mongoie.dtypes import MongoQuery, MongoPipeline, FilePath
from mongoie.exceptions import InvalidFilePathOrDir
from mongoie.chunky import ChunkedDataStream, chunk_generator
from mongoie.utils import (
    get_file_suffix,
    validate_file_path,
    list_files,
    get_list_of_files_with_supported_format,
)
from mongoie.log import get_logger
from mongoie.settings import Settings

logger = get_logger(__name__)


class MongoExporter:
    """Exports data to a file.

    Examples
    --------
    >>> exporter = MongoExporter(file_path="my_file.csv")
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
            The maximum size of each file to export to. If `None`,
            the data will be exported to a single file.
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
        Whether to normalize the data before exporting it. If `None`,
        the default value (`True`) will be used.
    file_size: Optional[int] = None
        The maximum size of each file to export to. If `None`,
        the data will be exported to a single file.
    **kwargs: Any
        Keyword arguments to pass to the exporter.

    Examples
    --------
    >>> collection = client.db.my_collection
    >>> export_collection(collection, file_path="my_collection.csv",
    normalize=True, file_size=1000)

    This will export the collection `my_collection` to the file `my_collection.csv`,
    split into multiple files of at most 1000 records each, with normalizing the data.
    """
    if not isinstance(collection, Collection):
        raise TypeError("collection should be pymongo.collection.Collection object")
    MongoExporter(
        file_path, file_size=file_size, normalize=normalize, **kwargs
    ).execute(data=collection.find({}), file_path=file_path, **kwargs)


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
    if not isinstance(cursor, Cursor):
        raise TypeError("cursor should by pymongo Cursor object")
    MongoExporter(file_path, file_size=file_size, normalize=normalize).execute(
        data=cursor, file_path=file_path, **kwargs
    )


def export_from_mongo(
    mongo_uri: str,
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
    mongo_uri: str
        The host to connect to.
    db: str
        The database to use.
    collection: str
        The collection to export.
    query: Optional[Union[MongoPipeline, MongoQuery]] = None
        A query to filter the collection before exporting it. If `None`,
        all documents in the collection will be exported.
    file_path: FilePath
        The path to the file to export the data to.
    normalize: Optional[bool] = None
        Whether to normalize the data before exporting it. If `None`,
        the default value (`True`) will be used.
    file_size: Optional[int] = None
        The maximum size of each file to export to. If `None`,
        the data will be exported to a single file.
    **kwargs: Any
        Keyword arguments to pass to the exporter.

    Returns
    -------
    None

    Examples
    --------
    >>> export_from_host(
    ...     mongo_uri="localhost:27017",
    ...     db="my_database",
    ...     collection="my_collection",
    ...     file_path="my_collection.csv",
    ...     normalize=True,
    ...     file_size=1000,
    ... )

    This will export the collection `my_collection` from the database `my_database`
    on the host `localhost`to the file `my_collection.csv`,
    split into multiple files of at most 1000 records each, with normalizing the data.

    """

    client = MongoConnector(mongo_uri)
    query = {} if query is None else query
    mongo_query_func: Callable = (
        client.aggregate if isinstance(query, list) else client.find
    )
    cursor = mongo_query_func(db, collection, query)
    export_cursor(cursor, file_path, normalize, file_size, **kwargs)


class MongoImporter:
    """Import file data to mongo collection"""

    def __init__(
        self,
        file_path: FilePath = None,
        dir_path: Union[os.PathLike, str] = None,
        file_extension: Optional[str] = None,
        recursive: Optional[bool] = False,
        pattern: Optional[str] = None,
        denormalized: Optional[bool] = None,
        denormalization_record_prefix: Optional[str] = None,
        clear_before: Optional[bool] = None,
        **kwargs: Any,
    ):
        """Initialize Mongo Importer

        Parameters
        ----------
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

        if not file_path and not dir_path:
            raise InvalidFilePathOrDir(
                "file_path or dir_path was not provided. Please provide file_path or dir_path"
            )

        if file_path and dir_path:
            logger.error("you provided both params file_path and dir_path")
            raise InvalidFilePathOrDir(
                "please provide only one parameter: either file_path or dir_path"
            )

        self._denormalized = denormalized or Settings.DENORMALIZE_IMPORTED_DATA
        self._record_prefix = (
            denormalization_record_prefix or Settings.DENORMALIZATION_RECORD_PREFIX
        )
        self._clear_before = clear_before or Settings.CLEAR_COLLECTION_BEFORE_IMPORT

        self._multi_files = True if dir_path else False

        if self._multi_files:
            self._files_paths: list = self._prep_files_list(
                dir_path, file_extension, recursive, pattern
            )
            self._file_suffix = get_file_suffix(self._files_paths[0], dot=False)
        else:
            self._file_suffix = get_file_suffix(file_path, dot=False)
            self._files_paths = [validate_file_path(file_path)]

        self.data_reader = get_reader(self._file_suffix)
        for k, v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def _prep_files_list(
        dir_path: Union[str, os.PathLike],
        file_extension: str,
        recursive: bool,
        pattern: str,
    ):
        """
        Prepare a list of files for further processing.

        This method checks if the directory exists, and if so, lists all files in
        the directory with the specified file extension. If the recursive flag is
        set, the method will also recursively list all files in subdirectories.

        Parameters
        ----------
        dir_path : Union[str, os.PathLike]
            The directory to list files in.
        file_extension : str
            The file extension to filter files by.
        recursive : bool
            Whether to recursively list files in subdirectories.
        pattern : str
            A regular expression pattern to filter files by.

        Returns
        -------
        List[Union[str, os.PathLike]]
            A list of files with the specified file extension and pattern.
        """

        files = list_files(dir_path, file_extension, recursive, pattern)
        return get_list_of_files_with_supported_format(
            files, ["json", "csv", "parquet"]
        )

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
        for file_path in self._files_paths:
            logger.info(f"importing {file_path} to mongo")
            data_gen = self.data_reader(
                file_path=file_path,
                denormalized=self._denormalized,
                record_prefix=self._record_prefix,
                **kwargs,
            )
            self._import(data=data_gen, **kwargs)


def list_mongo_databases(mongo_uri: str):
    """List all MongoDB databases on the given host.

    Parameters
    ----------
    mongo_uri : str
        The hostname or IP address of the MongoDB server.

    Returns
    -------
    list[str]
        A list of database names.

    """

    client = MongoConnector(mongo_uri)
    return client.list_database_names()


def list_mongo_collections(mongo_uri: str, db: str, regex=None):
    """List all MongoDB collections on the given host, matching the given criteria.

    Parameters
    ----------
    mongo_uri : str
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

    client = MongoConnector(mongo_uri)
    return client.list_collections(db, regex=regex)


def import_to_mongo_collection(
    collection: Collection,
    file_path: FilePath = None,
    dir_path: Union[str, os.PathLike] = None,
    file_extension: str = None,
    recursive: bool = False,
    pattern: str = None,
    denormalized: bool = None,
    denormalization_record_prefix: str = None,
    clear_before: bool = None,
    **kwargs,
):
    """Imports data from a file to a MongoDB collection.

    This function uses the `MongoImporter` class to import data from a file to a MongoDB collection.

    Parameters
    ----------
    collection: Collection
        The MongoDB collection to import data to.
    file_path: FilePath
        The path to the file to import data from.
    dir_path: Union[str, os.PathLike]
        The directory to list files in, if `file_path` is not provided.
    file_extension: str
        The file extension to filter files by, if `file_path` is not provided.
    recursive: bool
        Whether to recursively list files in subdirectories, if `file_path` is not provided.
    pattern: str
        A regular expression pattern to filter files by, if `file_path` is not provided.
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

    client = MongoConnector(host, db="some_db")
    my_collection = client.get_collection("my_collection")
    # Import the data from the "users.csv" file to the "my_collection" collection
    import_to_mongo_collection(
        collection=my_collection,
        file_path="users.csv",
    )
    ```
    """
    if not isinstance(collection, Collection):
        raise TypeError("collection should be pymongo.collection.Collection object")
    MongoImporter(
        file_path=file_path,
        dir_path=dir_path,
        file_extension=file_extension,
        recursive=recursive,
        pattern=pattern,
        denormalized=denormalized,
        denormalization_record_prefix=denormalization_record_prefix,
        clear_before=clear_before,
        **kwargs,
    ).execute(collection=collection)


def import_to_mongo(
    mongo_uri: str,
    *,
    db: str,
    collection: str,
    file_path: FilePath = None,
    dir_path: Union[str, os.PathLike] = None,
    file_extension: str = None,
    recursive: bool = False,
    pattern: str = None,
    denormalized: bool = None,
    denormalization_record_prefix: str = None,
    clear_before: bool = None,
    **kwargs,
):
    """Imports data from a file to MongoDB.

    This function uses the `MongoImporter` class to import data from a file to MongoDB.

    Parameters
    ----------
    mongo_uri: str
        The hostname or IP address of the MongoDB server.
    db: str
        The name of the database to import data to.
    collection: str
        The name of the collection to import data to.
    file_path: str
        The path to the file to import data from.
    dir_path: Union[str, os.PathLike]
        The directory to list files in, if `file_path` is not provided.
    file_extension: str
        The file extension to filter files by, if `file_path` is not provided.
    recursive: bool
        Whether to recursively list files in subdirectories, if `file_path` is not provided.
    pattern: str
        A regular expression pattern to filter files by, if `file_path` is not provided.
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
        mongo_uri="localhost:27017",
        db="my_database",
        collection="users",
        file_path="users.csv",
    )

    # Import all of the CSV files in the "data" directory to the
    "my_database" database, "users" collection
    import_to_mongo(
        mongo_uri="localhost:27017",
        db="my_database",
        collection="users",
        dir_path="data",
        file_extension=".csv",
        recursive=True,
    )

    # Import all of the JSON files in the "data" directory to the
     "my_database" database, "users" collection,
    # filtering the files by the regular expression pattern "users_*.json"
    import_to_mongo(
        mongo_uri="localhost:27017",
        db="my_database",
        collection="users",
        dir_path="data",
        file_extension=".json",
        recursive=True,
        pattern="users_*.json",
    )
    ```
    """

    client = MongoConnector(mongo_uri)
    coll = client.get_collection(db, collection)
    MongoImporter(
        file_path=file_path,
        dir_path=dir_path,
        file_extension=file_extension,
        recursive=recursive,
        pattern=pattern,
        denormalized=denormalized,
        denormalization_record_prefix=denormalization_record_prefix,
        clear_before=clear_before,
        **kwargs,
    ).execute(collection=coll)


__all__ = [
    "MongoImporter",
    "import_to_mongo",
    "import_to_mongo_collection",
    "export_from_mongo",
    "list_mongo_collections",
    "list_mongo_databases",
    "export_cursor",
    "export_collection",
]
