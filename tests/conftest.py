import pytest
import os
from mongoie.dal.mongo import MongoConnector
from mongoie.core.api import import_to_mongo


TEST_DIRECTORY = os.path.join(os.getcwd(), "tests")
TEST_DATA_DIRECTORY = os.path.join(TEST_DIRECTORY, "data")


@pytest.fixture
def mongo_client():
    return MongoConnector('localhost:27017', db='test_database')


@pytest.fixture(scope="session", autouse=False)
def _import_test_data_to_mongo():
    json_data = os.path.join(TEST_DATA_DIRECTORY, "data.json")
    import_to_mongo("localhost:27017", db="tests", collection="test_collection", file_path=json_data, skip_if_not_empty=True)

