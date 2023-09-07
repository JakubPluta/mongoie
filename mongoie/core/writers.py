from typing import Any

from bson import json_util

from mongoie.dtypes import FilePath
from mongoie.log import get_logger
from mongoie.utils import (
    remove_last_character,
    write_closing_bracket,
    ChunkedDataStream,
)

logger = get_logger(__name__)


def to_json(stream: ChunkedDataStream, file_path: FilePath) -> None:
    """Writes ChunkedDataStream data to a JSON file.

    ChunkedDataStream contains chunks of data which are yielded in lazy way.
    Chunk structure: List[Dict[Any, Any]

    Writes whole stream

    Args:
        stream: The ChunkedDataStream object.
        file_path: The path to the output JSON file.
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
    """Writes ChunkedDataStream data to a CSV file.

    Args:
        stream: The ChunkedDataStream object - stream of chunked pandas dataframe.
        file_path: The path to the output CSV file.
        sep: The delimiter used to separate the columns in the CSV file.
        **kwargs: Keyword arguments for the `pandas.DataFrame.to_csv()` function.

    """

    logger.debug(f"writing mongo data to {file_path}")
    rows = 0

    for chunk_idx, df in enumerate(stream.iter_as_normalized_dfs()):
        logger.debug(f"writing idx: {chunk_idx} with {len(df)} documents")
        header = True if chunk_idx == 0 else False
        df.to_csv(file_path, sep=sep, mode="a", header=header, index=False, **kwargs)
        rows += len(df)

    logger.debug(f"{rows} rows written to {file_path}")
