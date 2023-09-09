import csv
import dataclasses
import itertools
from typing import (
    Optional,
    Union,
    List,
    Iterable,
    Iterator,
    Any,
    Dict,
    AnyStr,
    Generator,
)
import os
from pathlib import Path
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
    """A data stream that yields chunks of data.

    Args:
        data: An iterable object that yields data.
        chunk_size: The size of each chunk.

    """

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
        """Iterate over the chunks of data as normalized Pandas DataFrames."""
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


def validate_file_path(file_path: FilePath) -> FilePath:
    """Validate the file path if it exists and has proper format.

    Parameters
    ----------
    file_path: The file path.

    Returns
    -------
    Path: The validated file path.

    Raises
    ------
    TypeError: If the file path is not a string or PathLike object.
    FileNotFoundError: If the file does not exist.

    """
    if not isinstance(file_path, (str, os.PathLike)):
        raise TypeError("path must be a string or PathLike object")
    file_path = Path(file_path)
    if file_path.exists() is False:
        raise FileNotFoundError(f"file {file_path} not exists")
    return file_path


def mkdir_if_not_exists(file_path: FilePath) -> None:
    """Create the directory if it does not exist.

    Parameters
    ----------
    file_path: The file path.

    """

    file_path = Path(file_path)
    directory = file_path.parent
    if directory.exists() is False:
        directory.mkdir(parents=True, exist_ok=True)


def get_delimiter(line: AnyStr) -> Optional[Any]:
    """
    Get the delimiter from a line of text.

    Parameters
    ----------
    line: AnyStr
        The line of text to parse.

    Returns
    -------
    Optional[Any]
        The delimiter, or `None` if it could not be determined.

    Raises
    ------
    ValueError
        If the line is not a string or bytes.
    """

    if not isinstance(line, str):
        raise ValueError("line must be a string or bytes")

    sniffer = csv.Sniffer()
    return sniffer.sniff(line).delimiter


def add_missing_suffix(file_path: FilePath, file_extension: str) -> FilePath:
    """
    Add file extension to file path.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the file.
    file_extension: str
        The file extension to add.

    Returns
    -------
    Union[str, Path]
        The file path with the extension added.
    """

    if file_path.endswith(file_extension):
        return file_path
    return (
        f"{file_path}{file_extension}"
        if file_path.endswith(".")
        else f"{file_path}.{file_extension}"
    )


def list_files(
    dir_path: str,
    ext: Optional[str] = None,
    recursive: bool = False,
    pattern: Optional[str] = None,
) -> List[Path]:
    """
    Get all paths matching the specified pattern in the specified directory.

    Parameters
    ----------
    dir_path: str
        The directory path.
    ext: Optional[str]
        The file extension to filter by.
    recursive: bool
        Whether to search recursively.
    pattern: Optional[str]
        The glob pattern to match.

    Returns
    -------
    List[Path]
        A list of paths matching the specified pattern.
    """
    pattern = "*" if pattern is None else pattern
    paths: Generator[Path] = (
        Path(dir_path).rglob(pattern) if recursive else Path(dir_path).glob(pattern)
    )
    f: Path
    files = [f.resolve() for f in filter(os.path.isfile, paths)]
    return (
        list(files) if ext is None else [file for file in files if ext in file.suffix]
    )


def get_file_suffix(path: FilePath, dot: bool = True) -> str:
    """
    Get the file suffix.

    Parameters
    ----------
    path: str
        The path to the file.
    dot: bool
        Whether to include the dot in the suffix.

    Returns
    -------
    str
        The file suffix.

    Raises
    ------
    TypeError
        If the path is not a string.
    """

    if not isinstance(path, (os.PathLike, str)):
        raise TypeError("path must be a string or os.PathLike")
    return Path(path).suffix if dot else Path(path).suffix[1:]


def resolve_file_path(file_path: str) -> os.PathLike:
    """
    Resolve a file path to an absolute path.

    Parameters
    ----------
    file_path : str
        The file path to resolve.

    Returns
    -------
    os.PathLike
        The absolute file path.
    """

    return Path(file_path).resolve()


def build_file_name(file_format: str, sep="_", *args) -> str:
    """
    Build a file name from a file format and a list of arguments.

    Parameters
    ----------
    file_format : str
        The file format.
    sep : str, optional
        The separator to use between the arguments. Defaults to "_".
    *args : str
        The arguments to use in the file name.

    Returns
    -------
    str
        The built file name.
    """

    name = f"{sep}".join([v for v in args])
    file_format = file_format[1:] if file_format.startswith(".") else file_format
    return f"{name}.{file_format}"


def build_file_path(file_name) -> Path:
    """Build a file path from a file name.

    Parameters
    ----------
    file_name : str
        The file name.

    Returns
    -------
    Path
        The file path.
    """
    return Path(__file__).parent / file_name
