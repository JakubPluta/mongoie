import dataclasses
import itertools
from typing import Optional, Union, List, Iterable, Iterator, Any, Dict

import pandas as pd

from mongoie.dtypes import FilePath, MongoDocument


def chunk_generator(iterable: Iterable, batch_size: int = 1000) -> Iterator[List[Any]]:
    """Yield chunks of an iterable.

    Parameters
    ----------
    iterable : Iterable
        The iterable to chunk.
    batch_size : int, optional
        The size of each chunk. Defaults to 1000.

    Returns
    -------
    Iterator[List[Any]]
        An iterator that yields chunks of the iterable.
    """
    it = iter(iterable)
    while True:
        # slice iterable [0, chunk_size] and returns generator
        chunk_it = itertools.islice(it, batch_size)
        try:
            first_el = next(chunk_it)
        except (
            StopIteration
        ):  # if iterator was exhausted and StopIteration raised breaks.
            return
        # joins first element and chunk without first element into one list. more: itertools.chain docs
        yield list(itertools.chain((first_el,), chunk_it))


@dataclasses.dataclass(repr=True)
class ChunkedDataStream:
    """A data stream that yields chunks of data."""

    data: Iterable[Any]
    chunk_size: int = 1000

    def __post_init__(self) -> None:
        """Initialize the data stream."""
        self.data = chunk_generator(self.data, self.chunk_size)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the chunks of data."""
        for chunk in self.data:
            yield chunk

    def iter_as_normalized_dfs(self):
        for chunk in self.data:
            yield json_normalize(chunk)


def remove_last_character(file_path: FilePath) -> None:
    """Open file and remove the last character from it.
    it's needed in mongo to json method"

    Parameters
    ----------
    file_path: The file path.

    """
    with open(file_path, "rb+") as file:
        file.seek(-1, 2)
        file.truncate()


def write_closing_bracket(file_path: FilePath) -> None:
    """Write the closing bracket to the file. It's needed for combining jsons in chunk into one json file

    Parameters
    ----------
    file_path: The file path.

    """
    with open(file_path, "a") as file:
        file.write("]")


def json_normalize(
    doc: Union[List[MongoDocument], MongoDocument],
    max_level: Optional[int] = None,
    **kwargs,
) -> pd.DataFrame:
    """Normalize json document and returns pandas dataframe

    Parameters
    ----------
    doc : dict or list of dicts. Unserialized JSON objects.
    max_level :  Max number of levels(depth of dict) to normalize. If None, normalizes all levels.

    Returns
    -------
    pandas.DataFrame

    """
    return pd.json_normalize(doc, max_level=max_level, **kwargs)


def df_denormalize(df: pd.DataFrame, record_prefix: str = ".") -> List[Dict[Any, Any]]:
    """The opposite of json_normalize
    more details: https://stackoverflow.com/questions/54776916/inverse-of-pandas-json-normalize
    """
    result = []
    for idx, row in df.iterrows():
        denormalized_records = {}

        for column_name, column_value in row.items():
            keys = column_name.split(record_prefix)
            current_record = denormalized_records
            for key in keys[
                :-1
            ]:  # create nested dictionaries up to the second-to-last level.
                current_record = current_record.setdefault(key, {})
            current_record[keys[-1]] = column_value  # last key
        result.append(denormalized_records)
    return result
