from typing import Callable, Union

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
    def __init__(
        self,
        host: str,
        *,
        db: str,
        collection: str,
        query: Optional[Union[MongoQuery, MongoPipeline]] = None,
        file_path: Optional[FilePath] = None,
    ):
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

    def _prep_chunks(self, **kwargs):
        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        return ChunkedDataStream(cursor)

    def _export(self, data, file_path, **kwargs):
        logger.info(
            f"writing data to {file_path}. using {self.data_writer.__name__} writer"
        )
        self.data_writer(data, file_path, **kwargs)

    def execute(self, **kwargs):
        self._export(self._prep_chunks(**kwargs), self.file_path, **kwargs)


class MongoImporter:
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

    def _prep_chunks(self, **kwargs):
        data_gen = self.data_importer(
            self.file_path,
            denormalized=self._denormalized,
            record_prefix=self._record_prefix,
            **kwargs,
        )
        return ChunkedDataStream(data_gen)

    def _import(self, data, file_path, **kwargs):
        logger.info(
            f"importing data to mongo from file: {file_path}. using {self.data_importer.__name__} importer"
        )
        self.data_importer(data, file_path, **kwargs)

    def execute(self, **kwargs):
        self._import(self._prep_chunks(**kwargs), self.file_path, **kwargs)


def export_from_mongo(
    host, *, db, collection, query: bool = None, file_path: str, **kwargs
):
    exporter = MongoExporter(
        host=host, db=db, collection=collection, query=query, file_path=file_path
    )
    exporter.execute(**kwargs)


def import_to_mongo(
    host,
    *,
    db,
    collection,
    file_path: str,
    denormalized: bool = None,
    denormalization_record_prefix: str = None,
    clear_before: str = None,
    **kwargs,
):
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
