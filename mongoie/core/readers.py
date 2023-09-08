from typing import Iterable

import ijson
import pandas as pd
from dtypes import FilePath
from log import get_logger
from mongoie.utils import (
    chunk_generator,
    df_denormalize,
)

logger = get_logger(__name__)


def read_json(file_path: FilePath, chunk_size: int = 1000):
    """read json file in chunks in lazy way"""

    def _read_json():
        with open(file_path, "rb") as f:
            for record in ijson.items(f, "item"):
                del record["_id"]
                yield record

    yield from chunk_generator(_read_json(), chunk_size)


def _read_csv(file_path: FilePath, chunk_size: int = 1000) -> Iterable:
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        yield chunk


def read_csv(
    file_path: FilePath,
    chunk_size: int = 1000,
    denormalized: bool = True,
    record_prefix: str = ".",
) -> Iterable:
    for chunk in _read_csv(file_path, chunk_size):
        chunk = (
            df_denormalize(chunk, record_prefix)
            if denormalized
            else chunk.to_dict("records")
        )
        yield chunk
