from typing import Iterator, Generator

import pandas as pd

from mongoie.utils import chunk_generator, ChunkedDataStream

import pytest


@pytest.mark.parametrize("start, end, batch_size", [
    (0,100, 20),
    (0,1000, 300),
    (1,100, 1),
    (5,5,5),
    (0,1,1)
])
def test_chunk_generator(start,end, batch_size):
    data = range(start, end)
    rng = end - start
    size, _ = divmod((rng), batch_size)
    size = size if _ == 0 else size + 1
    gen = chunk_generator(data, batch_size)
    consumed_gen = list(gen)
    assert isinstance(gen, Generator)
    assert len(consumed_gen) == size
    assert sum([len(x) for x in consumed_gen]) == rng


def test_chunked_data_stream_can_post_init():
    data = [1,2,3]
    cds = ChunkedDataStream(data, 1)
    assert isinstance(cds.data, Generator)
    assert isinstance(data, list)


def test_chunked_data_stream_can_iter_dfs():
    data = range(0,100)
    cds = ChunkedDataStream(data, 10)
    for df in cds.iter_as_df():
        assert isinstance(df, pd.DataFrame) and df.shape == (10, 1)
    with pytest.raises(StopIteration) as e:
         next(cds.data)


def test_chunked_data_stream_can_iter():
    data = range(0,100)
    cds = ChunkedDataStream(data, 10)
    for chunk in cds:
        assert isinstance(chunk, list) and len(chunk) == 10

