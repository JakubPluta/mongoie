import shutil

from mongoie.decorators import mkdir_decorator, inject_method, valid_file_path
import os


def test_mkdir_decorator(tmp_dir):
    @mkdir_decorator
    def some_func(file_path):
        print("func", file_path)

    p = os.path.join(tmp_dir, "file.json")
    some_func(p)
    assert os.path.exists(tmp_dir)
    shutil.rmtree(tmp_dir)


def test_inject_method():
    def get_data():
        return 1000

    @inject_method(get_data)
    def some_method(name):
        class Foo:
            def __init__(self, name):
                self.name = name

        return Foo(name=name)

    d = some_method("foo")
    hasattr(d, "get_data")
    assert d.get_data() == 1000
