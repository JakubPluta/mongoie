from typing import Callable, Union

from typing_extensions import Optional

from mongoie.core.writers import get_writer
from mongoie.dal.mongo import MongoConnector
from mongoie.dtypes import MongoQuery, MongoPipeline, FilePath
from mongoie.utils import (
    ChunkedDataStream,
    get_file_suffix,
    build_file_name,
    build_file_path,
)
from mongoie.log import get_logger

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

        self.data_writer = get_writer(self._file_suffix)

    def _prep_chunks(self, **kwargs):
        cursor = self.query_mongo(self.collection, self.query, **kwargs)
        return ChunkedDataStream(cursor)

    def _write(self, data, file_path, **kwargs):
        logger.info(
            f"writing data to {file_path}. using {self.data_writer.__name__} writer"
        )
        self.data_writer(data, file_path, **kwargs)

    def execute(self, **kwargs):
        self._write(self._prep_chunks(**kwargs), self.file_path, **kwargs)


def export_from_mongo(host, *, db, collection, query=None, file_path: str, **kwargs):
    exporter = MongoExporter(
        host=host, db=db, collection=collection, query=query, file_path=file_path
    )
    exporter.execute(**kwargs)


__all__ = ["MongoExporter", "export_from_mongo"]
