from typing import Any, Callable

from bson import json_util
from pymongo.collection import Collection

from mongoie.dtypes import FilePath
from mongoie.log import get_logger
from mongoie.utils import (
    remove_last_character,
    write_closing_bracket,
    ChunkedDataStream,
)
from mongoie.settings import Settings

logger = get_logger(__name__)


def to_json(stream: ChunkedDataStream, file_path: FilePath, **kwargs) -> None:
    """
    Writes a ChunkedDataStream to a JSON file.

    ChunkedDataStream contains chunks of data which are yielded in lazy way.
    Chunk structure: List[Dict[Any, Any]]

    Writes whole stream

    Args:
        stream: The ChunkedDataStream object.
        file_path: The path to the output JSON file.

    Returns:
        None

    """

    logger.debug(f"writing mongo data to {file_path}")
    docs = 0
    with open(file_path, "w") as file:
        file.write("[")
        for chunk_idx, chunk in enumerate(stream):
            logger.debug(f"writing idx: {chunk_idx} with {len(chunk)} documents")
            docs += len(chunk)
            file.write(json_util.dumps(chunk)[1:-1])
            file.write(",")

    remove_last_character(file_path)
    write_closing_bracket(file_path)
    logger.debug(f"{docs} documents written to {file_path}")


def to_csv(
    stream: ChunkedDataStream,
    file_path: FilePath,
    sep: str = ",",
    **kwargs: Any,
):
    """Writes a ChunkedDataStream to a CSV file.

    ChunkedDataStream contains chunks of data which are yielded in lazy way.
    Chunk structure: List[Dict[Any, Any]]

    Writes whole stream

    Args:
        stream: The ChunkedDataStream object.
        file_path: The path to the output CSV file.
        sep: The delimiter used to separate the columns in the CSV file.
        **kwargs: Keyword arguments for the `pandas.DataFrame.to_csv()` function.

    Returns:
        None

    """

    logger.debug(f"writing mongo data to {file_path}")
    rows = 0

    for chunk_idx, df in enumerate(stream.iter_as_normalized_dfs()):
        logger.debug(f"writing idx: {chunk_idx} with {len(df)} documents")
        header = True if chunk_idx == 0 else False
        df.to_csv(file_path, sep=sep, mode="a", header=header, index=False, **kwargs)
        rows += len(df)

    logger.debug(f"{rows} rows written to {file_path}")


def to_mongo(stream: ChunkedDataStream, collection: Collection, **kwargs):
    """Writes a ChunkedDataStream to a MongoDB collection.

    ChunkedDataStream contains chunks of data which are yielded in lazy way.
    Chunk structure: List[Dict[Any, Any]]

    Writes whole stream

    Args:
        stream: The ChunkedDataStream object.
        collection: The MongoDB collection object.
        **kwargs: Keyword arguments for the `pymongo.collection.insert_many()` function.

    Returns:
        None
    """
    logger.debug(f"writing json data to {collection.name}")
    rows = 0
    for chunk_idx, chunk in enumerate(stream):
        rows += len(chunk)
        logger.debug(f"writing idx: {chunk_idx} with {len(chunk)} documents")
        collection.insert_many(chunk, **kwargs)
    logger.debug(f"{rows} rows written to {collection.name}")


writers = {
    "csv": to_csv,
    #'mongo': to_mongo,
    "json": to_json,
}


def get_writer(file_suffix: str) -> Callable:
    """Gets a writer function for a given file suffix.

    Args:
        file_suffix: The file suffix.

    Returns:
        Callable: The writer function.
    """

    try:
        writer = writers[file_suffix]
    except KeyError:
        logger.warning(
            "couldn't find proper writer falling to default: {}".format(
                Settings.DEFAULT_WRITER_FORMAT
            )
        )
        writer = writers[Settings.DEFAULT_WRITER_FORMAT]
    return writer
