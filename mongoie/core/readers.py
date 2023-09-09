from typing import Iterable, Iterator, List, Any, Dict

import ijson
import pandas as pd
from dtypes import FilePath
from log import get_logger
from mongoie.utils import (
    chunk_generator,
    df_denormalize,
)

logger = get_logger(__name__)


def read_json(file_path: FilePath, chunk_size: int = 1000) -> Iterator[List[Any]]:
    """
    Read a JSON file in chunks in a lazy way.

    Parameters
    ----------
    file_path : FilePath
        The path to the JSON file.
    chunk_size : int, optional
        The size of each chunk. Defaults to 1000.

    Returns
    -------
    Iterator[dict]
        An iterator over the JSON records in the file, one chunk at a time.
    """

    def _read_json():
        with open(file_path, "rb") as f:
            for record in ijson.items(f, "item"):
                del record["_id"]
                yield record

    yield from chunk_generator(_read_json(), chunk_size)


def _read_csv(file_path: FilePath, chunk_size: int = 1000) -> Iterable[pd.DataFrame]:
    """
    Read a CSV file in chunks in a lazy way.

    Parameters
    ----------
    file_path : FilePath
        The path to the CSV file.
    chunk_size : int, optional
        The size of each chunk. Defaults to 1000.

    Returns
    -------
    Iterable[pd.DataFrame]
        An iterator over the Pandas DataFrames in the file, one chunk at a time.
    """

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        yield chunk


def read_csv(
    file_path: FilePath,
    chunk_size: int = 1000,
    denormalized: bool = True,
    record_prefix: str = ".",
) -> List[Dict[Any, Any]]:
    """
    Read a CSV file in chunks in a lazy way and denormalize the data.

    Parameters
    ----------
    file_path : FilePath
        The path to the CSV file.
    chunk_size : int, optional
        The size of each chunk. Defaults to 1000.
    denormalized : bool, optional
        Whether to denormalize the data. Defaults to True.
    record_prefix : str, optional
        The prefix to use for the denormalized data. Defaults to ".".

    Returns
    -------
    List[Dict[Any, Any]]
        An iterator over the denormalized data in the file, one chunk at a time.
    """

    for chunk in _read_csv(file_path, chunk_size):
        chunk = (
            df_denormalize(chunk, record_prefix)
            if denormalized
            else chunk.to_dict("records")
        )
        yield chunk
