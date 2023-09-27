from typing import Any, Callable

from bson import json_util
from pymongo.collection import Collection

from mongoie.dtypes import FilePath
from mongoie.log import get_logger
from mongoie.chunky import ChunkedDataStream, chunk_generator
from mongoie.utils import (
    remove_last_character,
    write_closing_bracket,
    add_number_prefix_to_file_path,
    mkdir_if_not_exists,
)
from mongoie.settings import Settings

logger = get_logger(__name__)


# signature
def _to(stream: ChunkedDataStream, file_path: FilePath, **kwargs):
    pass


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
    rows = 0
    with open(file_path, "w") as file:
        file.write("[")
        for chunk_idx, chunk in enumerate(stream):
            logger.debug(f"writing idx: {chunk_idx} with {len(chunk)} documents")
            rows += len(chunk)
            file.write(json_util.dumps(chunk)[1:-1])
            file.write(",")

    remove_last_character(file_path)
    write_closing_bracket(file_path)
    logger.info(f"{rows} documents written to {file_path}")


def to_csv(
    stream: ChunkedDataStream,
    file_path: FilePath,
    sep: str = ",",
    normalize: bool = True,
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
        normalize: Store data as normalized
        **kwargs: Keyword arguments for the `pandas.DataFrame.to_csv()` function.

    Returns:
        None

    """

    logger.info(f"writing mongo data to {file_path}")
    rows = 0
    stream_iterator = (
        stream.iter_as_df() if normalize is False else stream.iter_as_normalized_dfs()
    )
    for chunk_idx, df in enumerate(stream_iterator):
        logger.debug(f"writing idx: {chunk_idx} with {len(df)} documents")
        header = True if chunk_idx == 0 else False
        df.to_csv(
            path_or_buf=file_path,
            sep=sep,
            mode="a",
            header=header,
            index=False,
            **kwargs,
        )
        rows += len(df)

    logger.info(f"{rows} rows written to {file_path}")


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
    logger.info(f"writing json data to {collection.name}")
    rows = 0
    for chunk_idx, chunk in enumerate(stream):
        rows += len(chunk)
        logger.debug(f"writing idx: {chunk_idx} with {len(chunk)} documents")
        collection.insert_many(chunk, **kwargs)
    logger.info(f"{rows} rows written to {collection.name}")


def to_parquet(
    stream: ChunkedDataStream,
    file_path: FilePath,
    normalize: bool = True,
    **kwargs: Any,
) -> None:
    """Writes a ChunkedDataStream to a Parquet file.

    ChunkedDataStream contains chunks of data which are yielded in lazy way.
    Chunk structure: List[Dict[Any, Any]]

    Writes whole stream

    Args:
        stream: The ChunkedDataStream object.
        file_path: The path to the output Parquet file.
        normalize: Normalize data
        **kwargs: Keyword arguments for the `pandas.DataFrame.to_parquet()` function.

    Returns:
        None

    """
    logger.info(f"writing mongo data to {file_path}")
    rows = 0
    stream_iterator = (
        stream.iter_as_df() if normalize is False else stream.iter_as_normalized_dfs()
    )
    for chunk_idx, chunk in enumerate(stream_iterator):
        rows += len(chunk)
        logger.debug(f"writing idx: {chunk_idx} with {len(chunk)} documents")
        append = False if chunk_idx == 0 else True
        chunk.astype(dtype="str").to_parquet(
            file_path,
            engine="fastparquet",
            append=append,
            **kwargs,
        )
    logger.info(f"{rows} rows written to {file_path}")


def write_chunks(
    data: ChunkedDataStream,
    writer_func: Callable,
    file_path: FilePath,
    chunk_size: int = Settings.CHUNK_SIZE,
    multi_files: bool = False,
    **kwargs: Any,
):
    """Writes a ChunkedDataStream to a file or multiple files.

    Parameters
    ----------
    data: ChunkedDataStream
        The ChunkedDataStream to write.
    writer_func: Callable
        A function that writes a ChunkedDataStream to a file.
    file_path: str
        The path to the file to write to.
    chunk_size: int, optional
        The size of each chunk to write. The default value is 1000.
    multi_files: bool, optional
        Whether to write the data to multiple files. The default value is False.
    kwargs: Any, optional
        Additional keyword arguments to pass to the writer_func function.

    Returns
    -------
    None

    """
    if not isinstance(data, ChunkedDataStream):
        data = ChunkedDataStream(data)

    mkdir_if_not_exists(file_path)

    if multi_files is True:
        multi_file_chunks = (ChunkedDataStream(chunk, chunk_size) for chunk in data)
        for idx, chunk in enumerate(multi_file_chunks):
            new_file_path = add_number_prefix_to_file_path(file_path, f"chunk_{idx}")
            writer_func(chunk, file_path=new_file_path, **kwargs)
    else:
        writer_func(data, file_path=file_path, **kwargs)


def get_exporter(file_suffix: str) -> Callable:
    """Gets a writer function for a given file suffix.

    Args:
        file_suffix: The file suffix.

    Returns:
        Callable: The writer function.
    """

    try:
        writer = exporters[file_suffix]
    except KeyError:
        logger.warning(
            "couldn't find proper writer falling to default: {}".format(
                Settings.DEFAULT_EXPORT_FORMAT
            )
        )
        writer = exporters[Settings.DEFAULT_EXPORT_FORMAT]
    return writer


exporters = {
    "csv": to_csv,
    "json": to_json,
    "parquet": to_parquet,
}
