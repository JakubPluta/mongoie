import json
from typing import Iterable, Iterator, List, Any, Dict, Union, Callable

import ijson
import pandas as pd
from mongoie.dtypes import FilePath
from mongoie.log import get_logger
from pyarrow.parquet import ParquetFile
from mongoie.chunky import chunk_generator
from mongoie.utils import df_denormalize, get_file_suffix
from mongoie.decorators import valid_file_path
from mongoie.exceptions import InvalidFileExtension
from mongoie.settings import Settings

logger = get_logger(__name__)


def _read_json(file_path: FilePath) -> Iterator[Dict]:
    """
    Read a JSON in a lazy way.

    Parameters
    ----------
    file_path : FilePath
        The path to the JSON file.

    Returns
    -------
    Iterator[dict]
        An iterator over the JSON records in the file, one chunk at a time.
    """
    with open(file_path, "rb") as f:
        for record in ijson.items(f, "item"):
            if "_id" in record:
                del record["_id"]
            yield record


def read_json(
    file_path: FilePath, chunk_size: int = Settings.CHUNK_SIZE, **kwargs
) -> Iterator[List[Any]]:
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
     Iterator[List[Any]]
        An iterator over the JSON records in the file, one chunk at a time.
    """
    yield from chunk_generator(_read_json(file_path), chunk_size)


def _read_csv(
    file_path: FilePath, chunk_size: int = Settings.CHUNK_SIZE
) -> Iterable[pd.DataFrame]:
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
    chunk_size: int = Settings.CHUNK_SIZE,
    denormalized: bool = True,
    record_prefix: str = ".",
    **kwargs
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


def _read_parquet(file_path: FilePath, batch_size: int = Settings.CHUNK_SIZE):
    """Reads a parquet file in chunks.

    Args:
        file_path: The path to the parquet file.
        batch_size: The size of each chunk.

    Yields:
        A chunk of data from the parquet file.

    """

    parquet_file = ParquetFile(file_path)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield batch


def read_parquet(
    file_path: FilePath,
    chunk_size: int = Settings.CHUNK_SIZE,
    denormalized: bool = True,
    record_prefix: str = ".",
    **kwargs
) -> List[Dict[Any, Any]]:
    """Reads a parquet file in chunks and denormalizes the data.

    Args:
        file_path: The path to the parquet file.
        chunk_size: The size of each chunk.
        denormalized: Whether to denormalize the data.
        record_prefix: The prefix to use for denormalized records.

    Yields:
        A chunk of denormalized data from the parquet file.

    """

    for chunk in _read_parquet(file_path, chunk_size):
        df = chunk.to_pandas()
        df = (
            df_denormalize(df, record_prefix) if denormalized else df.to_dict("records")
        )
        yield df


@valid_file_path
def read_mongo_query_or_pipeline_from_json_file(
    file_path: FilePath,
) -> Union[dict, List[Dict]]:
    """Reads a MongoDB query or pipeline from a JSON file.

    Args:
        file_path: The path to the JSON file.

    Returns:
        The MongoDB query or pipeline as a dictionary or a list of dictionaries.

    Raises:
        InvalidFileExtension: If the file extension is not `.json`.

    """
    if get_file_suffix(file_path, dot=False) != "json":
        raise InvalidFileExtension("provided file is not json file")

    with open(file_path) as f:
        data = json.load(f)

    if data is None:
        logger.warning(
            "In current file, there is no valid query or pipeline specified. Setting up to default: {}"
        )
        return {}
    return data


readers = {
    "csv": read_csv,
    "json": read_json,
    "parquet": read_parquet,
}


def get_reader(file_suffix: str) -> Callable:
    """Gets a reader function for a given file suffix.

    Args:
        file_suffix: The file suffix.

    Returns:
        Callable: The writer function.
    """

    try:
        reader = readers[file_suffix]
    except KeyError:
        logger.warning(
            "couldn't find proper importer fallback to default: {}".format(
                Settings.DEFAULT_IMPORT_FORMAT
            )
        )
        reader = readers[Settings.DEFAULT_IMPORT_FORMAT]
    return reader
