from typing import Callable, Union, Any

from typing_extensions import Optional

from core.readers import get_importer
from mongoie.core.writers import get_exporter
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
from settings import Settings

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
            self._file_suffix = get_file_suffix(file_path)
            self.file_path = file_path

        self.data_writer = get_exporter(self._file_suffix)

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
        self.data_writer(data, file_path, **kwargs)

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
            The name of the database to export from.
        collection: str
            The name of the collection to export from.
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
        self.collection = collection

        self._file_suffix = get_file_suffix(file_path)
        self.file_path = validate_file_path(file_path)

        self.data_importer = get_importer(self._file_suffix)

        self._denormalized = denormalized or Settings.DENORMALIZE_IMPORTED_DATA
        self._record_prefix = (
            denormalization_record_prefix or Settings.DENORMALIZATION_RECORD_PREFIX
        )
        self._clear_before = clear_before or Settings.CLEAR_COLLECTION_BEFORE_IMPORT

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
        data_gen = self.data_importer(
            self.file_path,
            denormalized=self._denormalized,
            record_prefix=self._record_prefix,
            **kwargs,
        )
        return ChunkedDataStream(data_gen)

    def _import(self, data, file_path: FilePath, **kwargs: Any):
        """Imports data to mongo from file.

        Parameters
        ----------
        data:
            The data to import.
        file_path:
            The path to the file to import the data from.
        kwargs:
            Keyword arguments to pass to the data importer.
        """
        logger.info(
            f"importing data to mongo from file: {file_path}. using {self.data_importer.__name__} importer"
        )
        self.data_importer(data, file_path, **kwargs)

    def execute(self, **kwargs: Any):
        """Imports the data to mongo from the specified file path.
        Parameters
        ----------
        kwargs:
            Keyword arguments to pass to the `_import()` method.
        """
        self._import(self._prep_chunks(**kwargs), self.file_path, **kwargs)


def export_from_mongo(
    host: str,
    *,
    db,
    collection: str,
    query: Optional[Union[MongoPipeline, MongoQuery]] = None,
    file_path: FilePath,
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
    kwargs: Any
        Keyword arguments to pass to the exporter.

    Example
    ----------

    ```python
    from pymongo_export import export_from_mongo

    # Export the "users" collection from the "my_database" database to a file
    export_from_mongo(
        host="localhost":27017,
        db="my_database",
        collection="users",
        file_path="/path/to/export.csv",
    )
    ```
    """
    exporter = MongoExporter(
        host=host, db=db, collection=collection, query=query, file_path=file_path
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


__all__ = ["MongoExporter", "MongoImporter", "import_to_mongo", "export_from_mongo"]
