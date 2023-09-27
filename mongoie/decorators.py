import functools
from typing import Callable

from mongoie.core.writers import write_chunks
from mongoie.utils import validate_file_path, mkdir_if_not_exists
from functools import partial


def inject_method(func_to_inject: Callable) -> Callable:
    """Injects a method into the returned value of a function.

    Parameters
    ----------
    func_to_inject: Callable
        The method to inject.

    Returns
    -------
    Callable
        A decorator that injects the specified method into the returned value of a function.

    Examples
    --------
    >>> @inject_method(lambda x: x + 1)
    ... def add_one(x):
    ...     return x

    >>> add_one(1)
    2
    """

    def wrapper(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            result = func(*args, **kwargs)
            setattr(result, func_to_inject.__name__, func_to_inject)
            return result

        return inner

    return wrapper


def valid_file_path(func: Callable) -> Callable:
    """Decorator to validate the file path.

    Parameters
    ----------
    func: The function to decorate.

    Returns
    -------
    Callable: The decorated function.

    """

    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        file_path = validate_file_path(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper


def mkdir_decorator(func: Callable) -> Callable:
    """Decorator to create the directory if it does not exist.

    Parameters
    ----------
    func: The function to decorate.

    Returns
    -------
    Callable: The decorated function.

    """

    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        mkdir_if_not_exists(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper


__all__ = ["valid_file_path", "mkdir_decorator"]
