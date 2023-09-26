import json
import os
from typing import Iterator, Generator

import pandas as pd

from mongoie.utils import chunk_generator, ChunkedDataStream, validate_file_path, remove_last_character, \
    write_closing_bracket, json_normalize, df_denormalize, mkdir_if_not_exists, get_delimiter, add_missing_suffix, \
    add_number_prefix_to_file_path

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


def _create_temp_json_file(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f)


def test_should_remove_last_character():
    file_path = r"data\tmp.json"
    data = [{"a":"b"}]

    _create_temp_json_file(file_path, data)
    remove_last_character(file_path)
    with open(file_path, 'r') as f:
        d = f.read()
    os.remove(file_path)
    assert d == json.dumps(data)[:-1]


def test_should_write_closing_bracket():
    file_path = r"data\tmp.json"
    data = [{"a": "b"}]
    _create_temp_json_file(file_path, data)
    write_closing_bracket(file_path)
    with open(file_path, 'r') as f:
        d = f.read()
    os.remove(file_path)
    assert d == '[{"a": "b"}]]'


def test_should_properly_json_normalize():
    docs = [
        {"name": "John", "address": {"country" : "Spain", "city":"Barcelona"}}
    ]
    data = json_normalize(docs)
    assert (isinstance(data, pd.DataFrame)
            and set(data.columns) == {"name", "address.country", "address.city"})


def test_should_properly_df_denormalize():
    data = pd.DataFrame(
        data = [["John", "Spain", "Barcelona"]],
        columns=["name", "address.country", "address.city"]
    )
    assert df_denormalize(data) == [{'address': {'city': 'Barcelona', 'country': 'Spain'}, 'name': 'John'}]


@pytest.mark.parametrize("fp, exc", [
    (r"data\data.json", None),
    (5, TypeError),
    (r"notexisting\file.csv", FileNotFoundError)
])
def test_should_validate_file_path(fp, exc):
    if exc is None:
        assert validate_file_path(fp)
    else:
        with pytest.raises(exc):
            assert validate_file_path(fp)


def test_should_mkdir_if_not_exists():
    fp = r"data\to_delete\file.csv"
    mkdir_if_not_exists(fp)
    assert os.path.exists(r"data\to_delete")
    mkdir_if_not_exists(fp)
    assert os.path.exists(r"data\to_delete")
    os.rmdir(r"data\to_delete")


@pytest.mark.parametrize("line, delimiter", [
    ("some,fake,message", ","),
    ("some:fake:message", ":"),
    ("some\tfake\tmessage", "\t"),
])
def test_should_properly_detect_delimiter(line, delimiter):
    assert get_delimiter(line) == delimiter


@pytest.mark.parametrize("file_path, file_extension, result", [
    (r"file\file.csv", "csv", r"file\file.csv"),
    (r"file\file", "json", r"file\file.json"),
    (r"file\file", ".json", r"file\file.json"),
    (r"long1\long2\long3\file\file", ".parquet", r"long1\long2\long3\file\file.parquet"),

])
def test_should_properly_add_missing_suffix(file_path, file_extension, result):
    assert add_missing_suffix(file_path, file_extension) == result


def test_should_properly_list_files():
    pass


@pytest.mark.parametrize("fp, suffix, output", [
        (r"data\file.json", 1, r"data\file_1.json"),
        (r"data\file.json", "something", r"data\file_something.json"),
        (r"long\long\data\file.json", "__suffix__", r"long\long\data\file___suffix__.json"),
])
def test_should_properly_add_number_prefix_to_the_file_path(test_directory, fp, suffix, output):

    path_with_suffix = add_number_prefix_to_file_path(fp, suffix)
    assert str(path_with_suffix) == os.path.join(test_directory, output)
