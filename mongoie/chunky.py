import dataclasses
import itertools

from typing import List, Iterable, Iterator, Any
import pandas as pd

from mongoie.settings import Settings
from mongoie.utils import json_normalize


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
    itr = iter(iterable)
    while True:
        # slice iterable [0, chunk_size] and returns generator
        chunk_it = itertools.islice(itr, batch_size)
        try:
            first_el = next(chunk_it)
        except (
            StopIteration
        ):  # if iterator was exhausted and StopIteration raised breaks.
            return
        # joins first element and chunk without first element into
        # one list. more: itertools.chain docs
        yield list(itertools.chain((first_el,), chunk_it))


@dataclasses.dataclass(repr=True)
class ChunkedDataStream:
    """A data stream that yields chunks of data.

    Args:
        data: An iterable object that yields data.
        chunk_size: The size of each chunk.

    """

    data: Iterable[Any]
    chunk_size: int = Settings.CHUNK_SIZE

    def __post_init__(self) -> None:
        """Initialize the data stream."""
        self.data = chunk_generator(self.data, self.chunk_size)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the chunks of data."""
        for chunk in self.data:
            yield chunk

    def iter_as_normalized_dfs(self):
        """Iterate over the chunks of data as normalized Pandas DataFrames."""
        for chunk in self.data:
            yield json_normalize(chunk)

    def iter_as_df(self):
        """Iterate as dataframes"""
        for chunk in self.data:
            yield pd.DataFrame(chunk)
