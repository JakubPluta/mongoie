import pytest
import os
from mongoie.dal._mongo import MongoConnector
from mongoie.core.api import import_to_mongo
from tests import *
import shutil

@pytest.fixture
def test_data_directory():
    return TEST_DATA_DIRECTORY


@pytest.fixture
def test_directory():
    return TEST_DIRECTORY


@pytest.fixture
def test_data_json():
    return os.path.join(TEST_DATA_DIRECTORY, "data.json")


@pytest.fixture()
def test_data_multi_json():
    return os.path.join(TEST_DATA_DIRECTORY, "chunks")


@pytest.fixture
def mongo_uri():
    return "localhost:27017"


@pytest.fixture
def test_db():
    return "testdb"


@pytest.fixture
def test_collection():
    return "test_collection"


@pytest.fixture
def mongo_client(mongo_uri):
    return MongoConnector(mongo_uri)


@pytest.fixture
def setup_and_teardown_db(mongo_client, test_db, test_collection):
    mongo_client.drop_database(test_db)
    yield
    mongo_client.drop_database(test_db)


@pytest.fixture
def _import_test_data_to_mongo(test_db, test_collection, mongo_client):
    json_data = os.path.join(TEST_DATA_DIRECTORY, "data.json")
    import_to_mongo(
        "localhost:27017", db=test_db, collection=test_collection, file_path=json_data
    )
    yield
    mongo_client.drop_database(test_db)


@pytest.fixture
def tmp_dir(test_data_directory):
    return os.path.join(test_data_directory, "tmp")


@pytest.fixture
def prepare_tmpdir(tmp_dir):
    try:
        os.mkdir(tmp_dir)
    except Exception:
        pass
    yield
    shutil.rmtree(tmp_dir, ignore_errors=True)
